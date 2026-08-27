use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    CatalogAdapter, Component, ComponentId, Profile, RequirementLevel, Scenario,
    UnsupportedAdapterCapability,
};

/// Fully resolved profile target shared by every conformance probe.
pub(crate) struct ProbeTarget<'a> {
    pub(crate) adapter: &'a CatalogAdapter,
    pub(crate) component: &'a Component,
}

impl<'a> ProbeTarget<'a> {
    pub(crate) fn resolve_adapter(profile: &'a Profile, catalog: &ComponentId) -> Result<Self> {
        let adapter = profile
            .catalog_adapters
            .iter()
            .find(|adapter| adapter.catalog == *catalog)
            .with_context(|| format!("profile has no adapter for catalog `{catalog}`"))?;
        let component = profile
            .components
            .iter()
            .find(|component| component.id == *catalog)
            .with_context(|| format!("profile has no component for catalog `{catalog}`"))?;
        Ok(Self { adapter, component })
    }

    pub(crate) fn resolve(
        profile: &'a Profile,
        scenario: &Scenario,
        catalog: &ComponentId,
    ) -> Result<Self> {
        let target = Self::resolve_adapter(profile, catalog)?;
        let defined = profile
            .catalog_capabilities
            .iter()
            .map(|capability| &capability.id)
            .collect::<BTreeSet<_>>();
        for requirement in &scenario.capabilities {
            if !defined.contains(&requirement.capability) {
                bail!(
                    "scenario capability `{}` is absent from profile vocabulary",
                    requirement.capability
                );
            }
            if !target
                .adapter
                .capabilities
                .exercises(&requirement.capability)
                && target
                    .adapter
                    .capabilities
                    .limitation(&requirement.capability)
                    .is_none()
            {
                bail!(
                    "adapter does not classify scenario capability `{}`",
                    requirement.capability
                );
            }
        }
        Ok(target)
    }

    pub(crate) fn first_required_limitation(
        &self,
        scenario: &'a Scenario,
    ) -> Option<&'a UnsupportedAdapterCapability> {
        scenario
            .capabilities
            .iter()
            .filter(|requirement| requirement.level == RequirementLevel::Required)
            .find_map(|requirement| {
                self.adapter
                    .capabilities
                    .limitation(&requirement.capability)
            })
    }
}
