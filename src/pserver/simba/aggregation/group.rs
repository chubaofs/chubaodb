use crate::util::{error::*, time::* , entity::Field};
use crate::*;
use crate::util::coding::{slice_f64, slice_i64, sort_coding::*};

const DEF_DATE_FORMAT: &'static str = "%Y-%m-%d";


pub enum Group {
    Term(Term),
    Date(Date),
    Range(Range),
}

impl Group {
    pub fn new(name: String, params: Vec<String> , field:Field) -> ASResult<Group> {
        let group = match name.as_str() {
            "term" => match params.len() {
                1 => Group::Term(Term {
                    name: params[0].to_owned(),
                    field,
                }),
                _ => {
                    return result!(
                        Code::ParamError,
                        "group term:({},{:?}) must and only one field param",
                        name,
                        params
                    )
                }
            },
            "date" => match params.len() {
                1 => Group::Date(Date {
                    name: params[0].to_owned(),
                    field,
                    format: None,
                }),
                2 => Group::Date(Date {
                    name: params[0].to_owned(),
                    field,
                    format: Some(params[1].to_owned()),
                }),
                _ => {
                    return result!(
                        Code::ParamError,
                        "group date:({},{:?}) param Incorrect , example date(birthday)  or  date(birthday,%Y-%m-%d %H:%M:%S)",
                        name,
                        params
                    )
                }
            },
            "range" => {

                if params.len()<2{
                    return result!(Code::ParamError, "range:({:?}) need format start-end , example range(field,5-10, 6-20)", params) ;       
                }

                let mut range = Range{
                    name:params[0].clone(),
                    field,
                    ranges: Vec::with_capacity(params.len()-1),
                };
                for v in &params[1..]{
                    let split: Vec<&str> = v.split("-").collect() ;
                    if split.len() != 2{
                        return result!(Code::ParamError, "range:({:?}) need format start-end , example range(field,5-10)", params) ;       
                    }

                    let start = split[0].trim();
                    let start = if start.len() == 0{
                        f64::MIN 
                    }else{
                        start.parse::<f64>().map_err(|e| err!(Code::ParamError, "range:({:?}) need format start-end , example range(5-11, 6-20) err:{:?}",  params , e))?
                    };

                    let end = split[1].trim();
                    let end = if end.len() == 0{
                        f64::MAX 
                    }else{
                        end.parse::<f64>().map_err(|e| err!(Code::ParamError, "range:({:?}) need format start-end , example range(5-12, 6-20) err:{:?}",  params , e))?
                    };
                    range.ranges.push((start,end, v.to_string()));
                }
                Group::Range(range)
            },
            _ => return result!(Code::ParamError, "group:{} not define", name),
        };

        Ok(group)
    }

    pub fn coding(&self, value: &[u8]) -> ASResult<Vec<String>> {

        if value.len()==0{
            return Ok(vec![String::default()]);
        }

        match self{
            Group::Term(term) =>term.coding(value),
            Group::Date(date) =>date.coding(value),
            Group::Range(range) =>range.coding(value),
        }
    }

    pub fn name<'a>(&'a self) -> &'a str {
        match &self{
            Group::Term(Term{name , ..}) | Group::Date(Date{name,..}) | Group::Range(Range{name,..}) =>{
                name
            }
        }
    }
}

pub struct Term {
    name: String,
    field:Field,
}

impl Term{
    fn coding(&self, v: &[u8]) -> ASResult<Vec<String>> {
        let array = self.field.array();
        let result = match &self.field{
            Field::int(_) | Field::date(_)=>{
                if array {
                    let mut result = Vec::new();
                    for v in i64_arr_decoding(v) {
                        result.push(v.to_string());
                    }
                    result
                } else {
                    vec![slice_i64(v).to_string()]
                }
            }
            Field::float(_)=>{
                if array {
                    let mut result = Vec::new();
                    for v in f64_arr_decoding(v) {
                        result.push(v.to_string());
                    }
                    result
                } else {
                    vec![slice_f64(v).to_string()]
                }
            }
            Field::string(_) | Field::text(_)=>{
                if array {
                    let mut iter = str_arr_decoding(v);
                    let mut result = Vec::new();
                    while let Some(v) = iter.next() {
                        result.push(std::str::from_utf8(v).unwrap().to_string());
                    }
                    result
                } else {
                    vec![std::str::from_utf8(v).unwrap().to_string()]
                }
            }

            _ => return result!(Code::ParamError, "field:{} not support term agg", self.name),
            
        };
        Ok(result)
    }
}

pub struct Range {
    name: String,
    field:Field,
    ranges: Vec<(f64, f64, String)>,
}

impl Range{
    fn coding(&self, v: &[u8]) -> ASResult<Vec<String>> {
        let array = self.field.array();
        let mut result = Vec::new();
        let result = match &self.field{
            Field::int(_) | Field::date(_)=>{
                if array {
                    for v in i64_arr_decoding(v) {
                        let v = v as f64;
                        for range in self.ranges.iter(){
                            if v >= range.0 && v < range.1{
                                result.push(range.2.clone());
                            }
                        }
                    }
                } else {
                    let v = slice_i64(v) as f64;
                    for range in self.ranges.iter(){
                        if v >= range.0 && v < range.1{
                            result.push(range.2.clone());
                        }
                    }
                }
                result
            }
            Field::float(_)=>{
                let mut result = Vec::new();
                if array {
                    for v in f64_arr_decoding(v) {
                        for range in self.ranges.iter(){
                            if v >= range.0 && v < range.1{
                                result.push(range.2.clone());
                            }
                        }
                    }
                } else {
                    let v = slice_f64(v);
                    for range in self.ranges.iter(){
                        if v >= range.0 && v < range.1{
                            result.push(range.2.clone());
                        }
                    }
                }
                result
            }
            
            _ => return result!(Code::ParamError, "field:{} not support range agg", self.name),
        };


        Ok(result)
    }
}

pub struct Date {
    name: String,
    field:Field,
    format: Option<String>,
}

impl Date{
    fn coding<'b>(&self, v: &[u8]) -> ASResult<Vec<String>> {
        let array = self.field.array();
        let format = self.format.as_deref().unwrap_or(DEF_DATE_FORMAT) ;
        let result = match &self.field{
            Field::int(_) | Field::date(_)=>{
                if array {
                    let mut result = Vec::new();
                    for v in i64_arr_decoding(v) {
                        result.push(i64_time_str(v,format));
                    }
                    result
                } else {
                    vec![i64_time_str(slice_i64(v),format)]
                }
            }
            Field::float(_)=>{
                if array {
                    let mut result = Vec::new();
                    for v in f64_arr_decoding(v) {
                        result.push(i64_time_str(v as i64,format));
                    }
                    result
                } else {
                    vec![i64_time_str(slice_f64(v) as i64,format)]
                }
            }
            Field::string(_) | Field::text(_)=>{
                if array {
                    let mut iter = str_arr_decoding(v);
                    let mut result = Vec::new();
                    while let Some(v) = iter.next() {
                        result.push(str_time_str(std::str::from_utf8(v).unwrap().into(),format)?);
                    }
                    result
                } else {
                    vec![str_time_str(std::str::from_utf8(v).unwrap().into(),format,)?]
                }
            }
            _ => return result!(Code::ParamError, "field:{} not support data agg", self.name),
        };


        Ok(result)
    }
}