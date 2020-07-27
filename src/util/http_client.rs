// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
use crate::util::error::*;
use crate::*;
use log::{debug, info};
use serde_derive::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

pub async fn get_json<V: serde::de::DeserializeOwned>(url: &str, m_timeout: u64) -> ASResult<V> {
    info!("send get for url:{}", url);

    let resp = client_tout(m_timeout).get(url).send().await?;

    let http_code = resp.status().as_u16() as i32;
    if http_code != 200 {
        return result!(http_code, resp.text().await?);
    }

    Ok(resp.json::<V>().await?)
}

pub async fn post_json<T, V>(url: &str, m_timeout: u64, obj: &T) -> ASResult<V>
where
    T: serde::Serialize + ?Sized,
    V: serde::de::DeserializeOwned,
{
    info!("send post for url:{}", url);
    let resp = client_tout(m_timeout).post(url).json(obj).send().await?;

    let http_code = resp.status().as_u16() as i32;
    if http_code != 200 {
        return result!(http_code, resp.text().await?);
    }

    Ok(resp.json::<V>().await?)
}

#[derive(Deserialize, Clone, Debug)]
pub struct GraphqlResult {
    pub errors: Option<Value>,
    pub data: Option<Value>,
}

pub async fn graphql<V: std::fmt::Debug>(
    url: &str,
    timeout_sec: u64,
    query: &str,
    variables: Value,
    name: &str,
) -> ASResult<V>
where
    V: serde::de::DeserializeOwned,
{
    debug!(
        "send graphql for query:{} variables:{:?}",
        query,
        serde_json::to_string(&variables)
    );

    let result: GraphqlResult = post_json(
        url,
        timeout_sec,
        &json!({
            "query": query,
            "variables":variables,
        }),
    )
    .await?;

    if result.errors.is_some() {
        return result!(
            Code::InternalErr,
            serde_json::to_string(&result.errors.unwrap()).unwrap()
        );
    }

    if result.data.is_some() {
        match result.data.unwrap().get_mut(name) {
            Some(v) => {
                return serde_json::from_value(v.take())
                    .map_err(|e| err!(Code::InternalErr, e.to_string()));
            }
            None => {}
        }
    }

    return result!(Code::InternalErr, "data and errors both nil");
}

#[test]
fn test_graphql() {
    for _ in 1..100 {
        let json = json!({
            "query":"{partitionList(collectionName:\"t1\")}",
        });
        let f = post_json("http://127.0.0.1:7070", 100000, &json);
        let v: Value = async_std::task::block_on(f).unwrap();
        println!("{:#?}", v);
    }
}

fn client_tout(timeout: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout))
        .build()
        .unwrap()
}
