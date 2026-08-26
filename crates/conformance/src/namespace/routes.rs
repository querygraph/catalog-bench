use anyhow::Result;
use catalog_bench_common::contract::ComponentId;
use serde::{Deserialize, Serialize};

use crate::iceberg::{fixture_stem, NamespaceIdentifier};

const FIXTURE_PREFIX: &str = "cb_c104";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceFixture {
    pub id: String,
    pub primary: NamespaceIdentifier,
    pub sibling: NamespaceIdentifier,
    pub child: NamespaceIdentifier,
    pub missing_parent: NamespaceIdentifier,
}

impl NamespaceFixture {
    pub(super) fn new(catalog: &ComponentId, id: &str) -> Result<Self> {
        let stem = fixture_stem(FIXTURE_PREFIX, catalog, id)?;
        let primary = NamespaceIdentifier::single(format!("{stem}_a"))?;
        Ok(Self {
            id: id.to_owned(),
            sibling: NamespaceIdentifier::single(format!("{stem}_b"))?,
            child: primary.child("child")?,
            missing_parent: NamespaceIdentifier::single(format!("{stem}_missing"))?,
            primary,
        })
    }
}
