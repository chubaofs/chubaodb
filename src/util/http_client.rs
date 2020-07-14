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
use async_std::future;
use log::info;
use std::time::Duration;

pub async fn get_json<V: serde::de::DeserializeOwned>(url: &str, m_timeout: u64) -> ASResult<V> {
    info!("send get for url:{}", url);

    let mut resp = match future::timeout(Duration::from_millis(m_timeout), surf::get(url)).await {
        Err(e) => return result!(Code::Timeout, e.to_string()),
        Ok(resp) => conver(resp)?,
    };

    let http_code = resp.status().as_u16();
    if http_code != 200 {
        //try genererr
        let text = conver(resp.body_string().await)?;
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
            if let Some(code) = value.get("code") {
                if let Some(message) = value.get("message") {
                    return result!(code.as_u64().unwrap() as i32, message.to_string());
                };
            };
        };

        return result!(http_code as i32, text);
    }

    Ok(resp.body_json::<V>().await?)
}

pub async fn post_json<T, V>(url: &str, m_timeout: u64, obj: &T) -> ASResult<V>
where
    T: serde::Serialize + ?Sized,
    V: serde::de::DeserializeOwned,
{
    info!("send post for url:{}", url);

    let mut resp = match future::timeout(
        Duration::from_millis(m_timeout),
        surf::post(url).body_json(&obj).unwrap(),
    )
    .await
    {
        Err(e) => return result!(Code::Timeout, e.to_string()),
        Ok(resp) => conver(resp)?,
    };

    let http_code = resp.status().as_u16();
    if http_code != 200 {
        //try genererr
        let text = conver(resp.body_string().await)?;
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
            if let Some(code) = value.get("code") {
                if let Some(message) = value.get("message") {
                    return result!(code.as_i64().unwrap() as i32, message.to_string());
                };
            };
        };
        return result!(http_code as i32, text);
    }

    Ok(conver(resp.body_json::<V>().await)?)
}

// fn client_tout(timeout: u64) -> reqwest::Client {
//     reqwest::Client::builder()
//         .timeout(Duration::from_millis(timeout))
//         .build()
//         .unwrap()
// }
