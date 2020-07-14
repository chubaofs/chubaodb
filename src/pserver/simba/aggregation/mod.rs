pub mod function;
pub mod group;

use self::{function::*, group::*};
use crate::pserver::simba::engine::rocksdb::RocksDB;
use crate::*;
use crate::{
    pserverpb::*,
    util::{
        entity::{BytesField, Collection, Field, ID_BYTES},
        error::*,
    },
};
use std::cmp::Ordering;
use std::str::Chars;
use std::{collections::HashMap, sync::Arc};

pub mod group_type {
    type GroupType = &'static str;

    pub const TERM: GroupType = "term";
    pub const RANGE: GroupType = "range";
    pub const DATA: GroupType = "data";
}

pub struct Method {
    name: String,
    param: Vec<String>,
}

impl Method {
    fn name(&self) -> &str {
        match self.name.as_str() {
            "hits" => ID_BYTES,
            _ => self.param[0].as_str(),
        }
    }
    fn parse_method(str: &str) -> ASResult<Vec<Method>> {
        let mut result = vec![];
        if str.len() == 0 {
            return Ok(result);
        }

        let mut chars = str.chars();

        let method_name = |cs: &mut Chars| -> ASResult<Option<String>> {
            let mut name = String::new();
            loop {
                let v = cs.next();

                if v.is_none() {
                    if name.len() == 0 {
                        return Ok(None);
                    } else {
                        return result!(Code::ParamError, "param:{} incorrect format", str);
                    }
                }
                let v = v.unwrap();

                match v {
                    ')' => return result!(Code::ParamError, "param:{} incorrect format", str),
                    '(' => return Ok(Some(name)),
                    ' ' => {}
                    ',' => {
                        if name.len() > 0 {
                            name.push(v)
                        }
                    }
                    _ => name.push(v),
                }
            }
        };

        let method_param = |cs: &mut Chars| -> ASResult<Vec<String>> {
            let mut params = String::new();
            loop {
                let v = cs
                    .next()
                    .ok_or_else(|| err!(Code::ParamError, "param:{} incorrect format", str))?;
                match v {
                    '(' => return result!(Code::ParamError, "param:{} incorrect format", str),
                    ')' => return Ok(params.split(",").map(|s| s.to_owned()).collect()),
                    ' ' => {}
                    _ => params.push(v),
                }
            }
        };

        loop {
            let name = method_name(&mut chars)?;
            if name.is_none() {
                break;
            }
            let param = method_param(&mut chars)?;

            if param.len() == 0 {
                return result!(Code::ParamError, "param len is 0, {}", chars.as_str());
            }

            result.push(Method {
                name: name.unwrap(),
                param: param,
            });
        }

        Ok(result)
    }
}

// DataGroup, RangeGroup ,ValueGroup
#[derive(Default)]
pub struct Aggregator {
    pub size: usize,
    pub groups: Vec<Group>,
    pub functions: Vec<Function>,
    pub count: u64,
    pub result: HashMap<String, Vec<Function>>,
}

impl Aggregator {
    //structure Group by query string
    // group
    // term:name, example:?group=term(dept)
    // range: range(field,min,0,20,30,40,max) ?group=range(age,min,0,20,30,40,max)
    // data: data(field,yyyy-MM-dd) ?group=data(birthday,yyyy-MM-dd)
    // aggs
    // Stats(field) example ?fun=stats(age)
    // hits(size) example ?fun=hits(20)
    //full example ?group=term(dept),range(age,-0,0-20,20-30,30-40,40-),data(birthday,yyyy-MM)&fun=hits(20),stats(age)&size=10
    pub fn new(
        collection: &Arc<Collection>,
        size: usize,
        group_str: &str,
        fun_str: &str,
    ) -> ASResult<Self> {
        let mut agg = Aggregator {
            size: size,
            groups: vec![],
            functions: vec![],
            count: 0,
            result: HashMap::new(),
        };

        for ms in Method::parse_method(fun_str)? {
            let field = Self::get_field(&collection, ms.name())?;
            if !field.value() {
                return result!(
                    Code::ParamError,
                    "field:{} not value config is false, it can not to group"
                );
            }
            agg.functions.push(Function::new(
                ms.name,
                ms.param,
                collection.name.clone(),
                field,
            )?);
        }

        if agg.functions.len() == 0 {
            agg.functions.push(Function::Count(Count::new()));
        }

        for ms in Method::parse_method(group_str)? {
            let field = Self::get_field(&collection, ms.name())?;
            if !field.value() {
                return result!(
                    Code::ParamError,
                    "field:{} not value config is false, it can not to group"
                );
            }
            agg.groups.push(Group::new(ms.name, ms.param, field)?);
        }

        Ok(agg)
    }

    fn get_field(collection: &Arc<Collection>, name: &str) -> ASResult<Field> {
        if name == ID_BYTES {
            return Ok(Field::bytes(BytesField {
                name: name.to_string(),
                none: false,
            }));
        }
        for field in collection.fields.iter() {
            if field.name() == name {
                let target = field.clone();
                if !target.value() {
                    return result!(
                        Code::ParamError,
                        "field:{} not set value=true in schema",
                        name
                    );
                }
                return Ok(target);
            }
        }
        return result!(Code::ParamError, "field:{} not found in collection", name);
    }

    pub fn map(&mut self, key: &str, value: &Vec<&[u8]>) -> ASResult<()> {
        self.count += 1;
        for v in value {
            self._map(key, v)?;
        }
        Ok(())
    }

    pub fn _map(&mut self, key: &str, value: &[u8]) -> ASResult<()> {
        match self.result.get_mut(key) {
            None => {
                let mut aggs = self.functions.clone();
                for agg in aggs.iter_mut() {
                    agg.map(&value)?;
                }
                self.result.insert(String::from(key), aggs);
            }
            Some(aggs) => {
                for agg in aggs {
                    agg.map(&value)?;
                }
            }
        }
        Ok(())
    }

    //TODO: FIX it by into
    pub fn make_vec(
        &mut self,
        req: &Arc<QueryRequest>,
        db: &Arc<RocksDB>,
    ) -> ASResult<Vec<AggValues>> {
        let mut result = Vec::with_capacity(self.size);

        for (k, v) in self.result.iter() {
            result.push(AggValues {
                key: k.clone(),
                values: v
                    .iter()
                    .map(|f| f.make_agg_value(Some(db)))
                    .collect::<Vec<AggValue>>(),
            });
        }
        sort_and_resize(result, &req.sort, self.size)
    }
}

pub fn make_vec(
    map: HashMap<String, AggValues>,
    sort: &Vec<Order>,
    size: usize,
) -> ASResult<Vec<AggValues>> {
    println!("==============={}", map.len());

    let result = sort_and_resize(map.into_iter().map(|(_, v)| v).collect(), sort, size);

    println!("==============={:?}", result);
    result
}

fn sort_and_resize(
    mut result: Vec<AggValues>,
    sort: &Vec<Order>,
    size: usize,
) -> ASResult<Vec<AggValues>> {
    let sort = if sort.len() > 0 {
        let mut sort_code = 0;
        if sort.len() > 1 {
            return result!(
                Code::ParamError,
                "in agg sort value only support 1, example:sort=value:desc"
            );
        }

        if sort[0].name.eq_ignore_ascii_case("key") {
            sort_code += 2;
        }

        if sort[0].order.eq_ignore_ascii_case("asc") {
            sort_code += 1;
        }
        sort_code
    } else {
        0
    };

    result.sort_by(|a, b| {
        let mut ord = if sort & 2 == 2 {
            Ord::cmp(&a.key, &b.key)
        } else {
            cmp(&a.values, &b.values).unwrap()
        };
        if sort & 1 == 0 {
            ord = ord.reverse();
        }
        ord
    });

    if result.len() > size {
        unsafe { result.set_len(size) }
    }

    Ok(result)
}

fn cmp(a: &Vec<AggValue>, b: &Vec<AggValue>) -> ASResult<Ordering> {
    let len = a.len();
    if len != b.len() {
        return result!(
            Code::InternalErr,
            "agg has err sort values the len not same {}/{}",
            a.len(),
            b.len()
        );
    }

    for i in 0..len {
        let result = cmp_agg(&a[i], &b[i])?;
        if result != Ordering::Equal {
            return Ok(result);
        }
    }

    Ok(Ordering::Equal)
}

fn cmp_agg(a: &AggValue, b: &AggValue) -> ASResult<Ordering> {
    match (a.agg_value.as_ref(), b.agg_value.as_ref()) {
        (Some(agg_value::AggValue::Count(a)), Some(agg_value::AggValue::Count(b))) => {
            Ok(Ord::cmp(&a.count, &b.count))
        }
        (Some(agg_value::AggValue::Stats(a)), Some(agg_value::AggValue::Stats(b))) => {
            Ok(Ord::cmp(&a.count, &b.count))
        }
        (Some(agg_value::AggValue::Hits(a)), Some(agg_value::AggValue::Hits(b))) => {
            Ok(Ord::cmp(&a.count, &b.count))
        }
        _ => result!(
            Code::InternalErr,
            "agg has err agg type not same {:?}/{:?}",
            a,
            b
        ),
    }
}

#[test]
fn group_test() {
    let ms = Method::parse_method("term(dept),range(age,-0,0-20,20-30,30-40,40-)").unwrap();

    assert_eq!("term", ms[0].name);
    assert_eq!("dept", ms[0].param[0]);
    assert_eq!("range", ms[1].name);
    assert_eq!("age", ms[1].param[0]);
    assert_eq!(
        vec![
            String::from("-0"),
            String::from("0-20"),
            String::from("20-30"),
            String::from("30-40"),
            String::from("40-")
        ],
        ms[1].param[1..].to_vec()
    );
}
