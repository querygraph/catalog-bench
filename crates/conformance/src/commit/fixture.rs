use anyhow::Result;
use catalog_bench_common::contract::ComponentId;
use serde::{Deserialize, Serialize};

use crate::iceberg::{fixture_stem, NamespaceIdentifier};

pub(super) const FIXTURE_PREFIX: &str = "cb_c106";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitFixture {
    pub id: String,
    pub namespace: NamespaceIdentifier,
    pub table: String,
}

impl CommitFixture {
    pub(super) fn new(catalog: &ComponentId, id: &str) -> Result<Self> {
        Ok(Self {
            id: id.to_owned(),
            namespace: NamespaceIdentifier::single(fixture_stem(FIXTURE_PREFIX, catalog, id)?)?,
            table: "commit_correctness".to_owned(),
        })
    }
}
