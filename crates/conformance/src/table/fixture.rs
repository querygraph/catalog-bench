use anyhow::{bail, Result};
use catalog_bench_common::contract::ComponentId;
use serde::{Deserialize, Serialize};

use crate::iceberg::{fixture_stem, NamespaceIdentifier};

pub(super) const FIXTURE_PREFIX: &str = "cb_c105";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableIdentifier {
    pub namespace: NamespaceIdentifier,
    pub name: String,
}

impl TableIdentifier {
    pub(super) fn new(namespace: NamespaceIdentifier, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() || name.contains('/') {
            bail!("table names must be nonempty path-segment values");
        }
        Ok(Self { namespace, name })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableFixture {
    pub id: String,
    pub namespace: NamespaceIdentifier,
    pub missing_namespace: NamespaceIdentifier,
    pub primary: TableIdentifier,
    pub sibling: TableIdentifier,
    pub renamed: TableIdentifier,
    pub registered: TableIdentifier,
    pub missing: TableIdentifier,
}

impl TableFixture {
    pub(super) fn new(catalog: &ComponentId, id: &str) -> Result<Self> {
        let stem = fixture_stem(FIXTURE_PREFIX, catalog, id)?;
        let namespace = NamespaceIdentifier::single(stem.clone())?;
        Ok(Self {
            id: id.to_owned(),
            missing_namespace: NamespaceIdentifier::single(format!("{stem}_missing"))?,
            primary: TableIdentifier::new(namespace.clone(), "primary")?,
            sibling: TableIdentifier::new(namespace.clone(), "sibling")?,
            renamed: TableIdentifier::new(namespace.clone(), "renamed")?,
            registered: TableIdentifier::new(namespace.clone(), "registered")?,
            missing: TableIdentifier::new(namespace.clone(), "missing")?,
            namespace,
        })
    }

    pub(super) fn candidates(&self) -> [&TableIdentifier; 4] {
        [
            &self.primary,
            &self.renamed,
            &self.sibling,
            &self.registered,
        ]
    }
}
