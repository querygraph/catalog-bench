use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{CatalogAdapter, ComponentId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::form_urlencoded;
use url::Url;

use crate::config::PrefixResolution;

pub(super) const DEFAULT_SEPARATOR: &str = "%1F";
const FIXTURE_PREFIX: &str = "cb_c104";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespaceIdentifier(Vec<String>);

impl NamespaceIdentifier {
    pub(super) fn from_parts(parts: Vec<String>) -> Result<Self> {
        if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
            bail!("namespace identifiers must contain nonempty parts");
        }
        Ok(Self(parts))
    }

    pub(super) fn single(part: String) -> Result<Self> {
        Self::from_parts(vec![part])
    }

    pub(super) fn child(&self, part: &str) -> Result<Self> {
        let mut parts = self.0.clone();
        parts.push(part.to_owned());
        Self::from_parts(parts)
    }

    pub(super) fn parts(&self) -> &[String] {
        &self.0
    }
}

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
        validate_fixture_id(id)?;
        let catalog = catalog.as_str().replace('-', "_");
        let stem = format!("{FIXTURE_PREFIX}_{catalog}_{id}");
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

fn validate_fixture_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 24 {
        bail!("fixture id must contain 1 to 24 characters");
    }
    if !id
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!("fixture id must contain only lowercase ASCII letters, digits, and underscores");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum NamespaceSeparatorResolution {
    Default {
        encoded: String,
    },
    Configured {
        json_pointer: String,
        encoded: String,
    },
    Failed {
        explanation: String,
    },
    NotEvaluated {
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub(super) struct NamespaceCodec {
    separator: String,
}

impl NamespaceCodec {
    pub(super) fn resolve(config: Option<&Value>) -> (NamespaceSeparatorResolution, Option<Self>) {
        let configured = [
            "/overrides/namespace-separator",
            "/defaults/namespace-separator",
        ]
        .into_iter()
        .find_map(|pointer| {
            config
                .and_then(|body| body.pointer(pointer))
                .map(|value| (pointer, value))
        });

        let (resolution, encoded) = match configured {
            Some((pointer, Value::String(encoded))) => (
                NamespaceSeparatorResolution::Configured {
                    json_pointer: pointer.to_owned(),
                    encoded: encoded.clone(),
                },
                encoded.as_str(),
            ),
            Some((pointer, _)) => {
                return (
                    NamespaceSeparatorResolution::Failed {
                        explanation: format!(
                            "config namespace separator at `{pointer}` must be a string"
                        ),
                    },
                    None,
                );
            }
            None if config.is_some() => (
                NamespaceSeparatorResolution::Default {
                    encoded: DEFAULT_SEPARATOR.to_owned(),
                },
                DEFAULT_SEPARATOR,
            ),
            None => {
                return (
                    NamespaceSeparatorResolution::NotEvaluated {
                        reason: "config response has no captured JSON body".to_owned(),
                    },
                    None,
                );
            }
        };

        match decode_separator(encoded) {
            Ok(separator) => (resolution, Some(Self { separator })),
            Err(error) => (
                NamespaceSeparatorResolution::Failed {
                    explanation: error.to_string(),
                },
                None,
            ),
        }
    }

    fn join(&self, namespace: &NamespaceIdentifier) -> String {
        namespace.parts().join(&self.separator)
    }
}

fn decode_separator(encoded: &str) -> Result<String> {
    let query = format!("separator={encoded}");
    let decoded = form_urlencoded::parse(query.as_bytes())
        .next()
        .map(|(_, value)| value.into_owned())
        .context("decode namespace separator")?;
    if decoded.chars().count() != 1 {
        bail!("namespace separator `{encoded}` must decode to exactly one character");
    }
    if decoded.chars().any(|character| "/?#".contains(character)) {
        bail!("namespace separator `{encoded}` cannot be a URL structural character");
    }
    Ok(decoded)
}

pub(super) struct CatalogRoutes {
    base_url: String,
    prefix: Option<String>,
    codec: NamespaceCodec,
}

impl CatalogRoutes {
    pub(super) fn new(
        adapter: &CatalogAdapter,
        prefix: &PrefixResolution,
        codec: NamespaceCodec,
    ) -> Result<Self> {
        let prefix = match prefix {
            PrefixResolution::Unprefixed => None,
            PrefixResolution::Static { value } | PrefixResolution::Negotiated { value, .. } => {
                Some(value.clone())
            }
            PrefixResolution::Failed { explanation } => {
                bail!("cannot build namespace routes: {explanation}")
            }
            PrefixResolution::NotEvaluated { reason } => {
                bail!("cannot build namespace routes: {reason}")
            }
        };
        Ok(Self {
            base_url: adapter.endpoint.base_url.clone(),
            prefix,
            codec,
        })
    }

    pub(super) fn collection(&self) -> Result<Url> {
        self.resource_url(&["namespaces"])
    }

    pub(super) fn namespace(&self, namespace: &NamespaceIdentifier) -> Result<Url> {
        let encoded = self.codec.join(namespace);
        self.resource_url(&["namespaces", &encoded])
    }

    pub(super) fn properties(&self, namespace: &NamespaceIdentifier) -> Result<Url> {
        let encoded = self.codec.join(namespace);
        self.resource_url(&["namespaces", &encoded, "properties"])
    }

    pub(super) fn under_parent(&self, parent: &NamespaceIdentifier) -> Result<Url> {
        let mut url = self.collection()?;
        url.query_pairs_mut()
            .append_pair("parent", &self.codec.join(parent));
        Ok(url)
    }

    pub(super) fn page(&self, token: &str, size: usize) -> Result<Url> {
        let mut url = self.collection()?;
        url.query_pairs_mut()
            .append_pair("pageToken", token)
            .append_pair("pageSize", &size.to_string());
        Ok(url)
    }

    fn resource_url(&self, tail: &[&str]) -> Result<Url> {
        let mut url = Url::parse(&self.base_url)
            .with_context(|| format!("invalid adapter base URL `{}`", self.base_url))?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("adapter base URL cannot carry path segments"))?;
            segments.push("v1");
            if let Some(prefix) = &self.prefix {
                segments.push(prefix);
            }
            segments.extend(tail.iter().copied());
        }
        Ok(url)
    }
}
