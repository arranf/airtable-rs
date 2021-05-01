//! License: MIT

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

use tracing::debug;

const URL: &str = "https://api.airtable.com/v0";
#[derive(Debug)]
pub struct Base<T: Record> {
    table: String,
    api_key: String,
    app_key: String,
    phantom: PhantomData<T>,
}

pub fn new<T>(api_key: &str, app_key: &str, table: &str) -> Base<T>
where
    T: Record,
{
    Base {
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

        let url = &format!("{}/{}/{}", URL, self.base.app_key, self.base.table);
        let mut req = ureq::get(&url);

        if self.offset.is_some() {
            req = req.query("offset", self.offset.as_ref().unwrap());
        }

        if self.query_builder.view.is_some() {
            req = req.query("view", self.query_builder.view.as_ref().unwrap());
        }

        if self.query_builder.formula.is_some() {
            req = req.query(
                "filterByFormula",
                self.query_builder.formula.as_ref().unwrap(),
            );
        }

        if self.query_builder.sort.is_some() {
            for (i, ref sort) in self.query_builder.sort.as_ref().unwrap().iter().enumerate() {
                req = req.query(&format!("sort[{}][field]", i), &sort.0);
                req = req.query(&format!("sort[{}][direction]", i), &sort.1.to_string());
            }
        }

        debug!("Blocking on get!");
        let results: RecordPage<T> = req
            .set("Authorization", &format!("Bearer {}", &self.base.api_key))
            .set("Content-Type", "application/json")
            .call()
            .ok()?
            .into_json()
            .ok()?;

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
            view: None,
            formula: None,
            sort: None,
        }
    }

    pub async fn create(&self, record: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        let url = format!("{}/{}/{}", URL, self.app_key, self.table);

        let serializing_record = SRecord {
            id: String::new(),
            fields: record,
        };

        let json = serde_json::to_string(&serializing_record)?;

        ureq::post(&url)
            .set("Authorization", &format!("Bearer {}", &self.api_key))
            .set("Content-Type", "application/json")
            .send_string(&json)?;
        Ok(())
    }

    // TODO: Perhaps pass a mutable reference to allow updating computed fields when someone does
    // an update?
    //
    // TODO: Include the error body in the error.
    pub async fn update(&self, record: &T) -> Result<()>
    where
        T: serde::Serialize,
    {
        let url = format!("{}/{}/{}/{}", URL, self.app_key, self.table, record.id());

        let serializing_record = SRecord {
            id: record.id().to_owned(),
            fields: record,
        };

        let json = serde_json::to_string(&serializing_record)?;

        ureq::request("PATCH", &url)
            .set("Authorization", &format!("Bearer {}", &self.api_key))
            .set("Content-Type", "application/json")
            .send_string(&json)?;

        Ok(())
    }
}
