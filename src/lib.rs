//! License: MIT
#![allow(dead_code)]
extern crate failure;
extern crate reqwest;
extern crate serde;
extern crate serde_json;

#[cfg(test)]
extern crate mockito;

use failure::Error;
use futures;
use reqwest::header;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

const URL: &str = "https://api.airtable.com/v0";

use tracing::debug;

#[derive(Debug)]
pub struct Base<T: Record> {
    http_client: reqwest::Client,

    table: String,
    api_key: String,
    app_key: String,

    phantom: PhantomData<T>,
}

pub fn new<T>(api_key: &str, app_key: &str, table: &str) -> Base<T>
where
    T: Record,
{
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_str(&format!("Bearer {}", &api_key)).expect("invalid api key"),
    );

    headers.insert(
        reqwest::header::CONTENT_TYPE,
        header::HeaderValue::from_str("application/json").expect("invalid content type"),
    );

    let http_client = reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .expect("unable to create client");

    Base {
        http_client,
        api_key: api_key.to_owned(),
        app_key: app_key.to_owned(),
        table: table.to_owned(),
        phantom: PhantomData,
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SRecord<T> {
    #[serde(default, skip_serializing)]
    id: String,
    fields: T,
}

#[derive(Deserialize, Debug)]
struct RecordPage<T> {
    records: Vec<SRecord<T>>,

    #[serde(default)]
    offset: String,
}

pub struct Paginator<'base, T: Record> {
    base: &'base Base<T>,
    // TODO: Move the offset to query_builder
    offset: Option<String>,
    iterator: std::vec::IntoIter<T>,
    query_builder: QueryBuilder<'base, T>,
}

impl<'base, T> Iterator for Paginator<'base, T>
where
    for<'de> T: Deserialize<'de>,
    T: Record,
{
    type Item = T;
    // This somewhat masks errors..
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iterator.next();
        if next.is_some() {
            return next;
        }

        if self.offset.is_none() {
            return None;
        }

        let mut url = Url::parse(&format!(
            "{}/{}/{}",
            URL, self.base.app_key, self.base.table
        ))
        .unwrap();
        url.query_pairs_mut()
            .append_pair("offset", self.offset.as_ref().unwrap());

        if self.query_builder.view.is_some() {
            url.query_pairs_mut()
                .append_pair("view", self.query_builder.view.as_ref().unwrap());
        }

        if self.query_builder.formula.is_some() {
            url.query_pairs_mut().append_pair(
                "filterByFormula",
                self.query_builder.formula.as_ref().unwrap(),
            );
        }

        if self.query_builder.sort.is_some() {
            for (i, ref sort) in self.query_builder.sort.as_ref().unwrap().iter().enumerate() {
                url.query_pairs_mut()
                    .append_pair(&format!("sort[{}][field]", i), &sort.0);
                url.query_pairs_mut()
                    .append_pair(&format!("sort[{}][direction]", i), &sort.1.to_string());
            }
        }

        let response = futures::executor::block_on(self.base.http_client.get(url.as_str()).send());
        debug!("Response from get {:?}", response);
        let response = response.ok()?;

        let json = futures::executor::block_on(response.json());
        debug!("Response JSON is ok?: {}", json.is_ok());
        let results: RecordPage<T> = json.ok()?;

        if results.offset.is_empty() {
            self.offset = None;
        } else {
            self.offset = Some(results.offset);
        }

        let window: Vec<T> = results
            .records
            .into_iter()
            .map(|record| {
                let mut record_t: T = record.fields;
                record_t.set_id(record.id);
                record_t
            })
            .collect();

        self.iterator = window.into_iter();
        self.iterator.next()
    }
}

pub trait Record {
    fn set_id(&mut self, error: String);
    fn id(&self) -> &str;
}

pub enum SortDirection {
    Descending,
    Ascending,
}

impl ToString for SortDirection {
    fn to_string(&self) -> String {
        match self {
            SortDirection::Descending => String::from("desc"),
            SortDirection::Ascending => String::from("asc"),
        }
    }
}

pub struct QueryBuilder<'base, T: Record> {
    base: &'base Base<T>,

    fields: Option<Vec<String>>,
    view: Option<String>,
    formula: Option<String>,

    // TODO: Second value here should be an enum.
    sort: Option<Vec<(String, SortDirection)>>,
}

impl<'base, T> QueryBuilder<'base, T>
where
    for<'de> T: Deserialize<'de>,
    T: Record,
{
    pub fn view(mut self, view: &str) -> Self {
        self.view = Some(view.to_owned());
        self
    }

    pub fn formula(mut self, formula: &str) -> Self {
        self.formula = Some(formula.to_owned());
        self
    }

    pub fn sort(mut self, field: &str, direction: SortDirection) -> Self {
        match self.sort {
            None => {
                self.sort = Some(vec![(field.to_owned(), direction)]);
            }
            Some(ref mut sort) => {
                let tuple = (field.to_owned(), direction);
                sort.push(tuple);
            }
        };
        self
    }
}

impl<'base, T> IntoIterator for QueryBuilder<'base, T>
where
    for<'de> T: Deserialize<'de>,
    T: Record,
{
    type Item = T;
    type IntoIter = Paginator<'base, T>;

    fn into_iter(self) -> Self::IntoIter {
        Paginator {
            base: &self.base,
            offset: Some("".to_owned()),
            iterator: vec![].into_iter(),
            query_builder: self,
        }
    }
}

impl<T> Base<T>
where
    for<'de> T: Deserialize<'de>,
    T: Record,
{
    pub fn query(&self) -> QueryBuilder<T> {
        QueryBuilder {
            base: self,
            fields: None,
            view: None,
            formula: None,
            sort: None,
        }
    }

    pub async fn create(&self, record: &T) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        let url = format!("{}/{}/{}", URL, self.app_key, self.table);

        let serializing_record = SRecord {
            id: String::new(),
            fields: record,
        };

        let json = serde_json::to_string(&serializing_record)?;

        self.http_client
            .post(&url)
            .body(json)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    // TODO: Perhaps pass a mutable reference to allow updating computed fields when someone does
    // an update?
    //
    // TODO: Include the error body in the error.
    pub async fn update(&self, record: &T) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        let url = format!("{}/{}/{}/{}", URL, self.app_key, self.table, record.id());

        let serializing_record = SRecord {
            id: record.id().to_owned(),
            fields: record,
        };

        let json = serde_json::to_string(&serializing_record)?;

        self.http_client
            .patch(&url)
            .body(json)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}
