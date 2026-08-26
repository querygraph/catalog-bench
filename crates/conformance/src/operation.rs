use std::collections::BTreeMap;

use catalog_bench_common::contract::{AssertionOutcome, CapabilityId};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::evidence::{HttpResponseTranscript, ProbeFailure};
use crate::sanitize::sanitize_json;
use crate::transport::{execute_json_request, REDACTED};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationHttpRequestTranscript {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum OperationExecution {
    Attempted {
        request: Box<OperationHttpRequestTranscript>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<HttpResponseTranscript>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<ProbeFailure>,
    },
    NotAttempted {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationTranscript {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<CapabilityId>,
    #[serde(flatten)]
    pub execution: OperationExecution,
}

#[derive(Clone)]
pub(crate) struct Observation {
    pub(crate) status: Option<u16>,
    pub(crate) private_json: Option<Value>,
    pub(crate) failure: Option<ProbeFailure>,
}

pub(crate) struct OperationRecorder<'a> {
    client: &'a Client,
    bearer_token: Option<&'a str>,
    sensitive_values: &'a [String],
    operations: Vec<OperationTranscript>,
    redactions: Vec<String>,
}

impl<'a> OperationRecorder<'a> {
    pub(crate) fn new(
        client: &'a Client,
        bearer_token: Option<&'a str>,
        sensitive_values: &'a [String],
    ) -> Self {
        Self {
            client,
            bearer_token,
            sensitive_values,
            operations: Vec::new(),
            redactions: Vec::new(),
        }
    }

    pub(crate) async fn attempt(
        &mut self,
        id: impl Into<String>,
        capability: Option<&str>,
        method: Method,
        url: Url,
        body: Option<Value>,
    ) -> Observation {
        let id = id.into();
        let mut headers = BTreeMap::from([("accept".to_owned(), "application/json".to_owned())]);
        if body.is_some() {
            headers.insert("content-type".to_owned(), "application/json".to_owned());
        }
        if self.bearer_token.is_some() {
            headers.insert("authorization".to_owned(), REDACTED.to_owned());
            self.redactions
                .push(format!("operations.{id}.request.headers.authorization"));
        }

        let sanitized_url = sanitize_json(Value::String(url.to_string()), self.sensitive_values);
        if !sanitized_url.redactions.is_empty() {
            self.redactions.push(format!("operations.{id}.request.url"));
        }
        let sanitized_url = sanitized_url
            .value
            .as_str()
            .expect("sanitizing a JSON string preserves its type")
            .to_owned();
        let sanitized_body = body.as_ref().map(|body| {
            let sanitized = sanitize_json(body.clone(), self.sensitive_values);
            self.redactions.extend(
                sanitized
                    .redactions
                    .iter()
                    .map(|path| format!("operations.{id}.request.body{path}")),
            );
            sanitized.value
        });
        let captured = execute_json_request(
            self.client,
            method.clone(),
            url,
            self.bearer_token,
            body.as_ref(),
            self.sensitive_values,
            &id,
        )
        .await;
        self.redactions.extend(
            captured
                .redactions
                .iter()
                .map(|path| format!("operations.{id}.{path}")),
        );
        let observation = Observation {
            status: captured.response.as_ref().map(|response| response.status),
            private_json: captured.private_json,
            failure: captured.failure.clone(),
        };
        self.operations.push(OperationTranscript {
            id,
            capability: capability.map(CapabilityId::new),
            execution: OperationExecution::Attempted {
                request: Box::new(OperationHttpRequestTranscript {
                    method: method.as_str().to_owned(),
                    url: sanitized_url,
                    headers,
                    body: sanitized_body,
                }),
                response: captured.response,
                failure: captured.failure,
            },
        });
        observation
    }

    pub(crate) fn skip(&mut self, id: impl Into<String>, capability: Option<&str>, reason: &str) {
        self.operations.push(OperationTranscript {
            id: id.into(),
            capability: capability.map(CapabilityId::new),
            execution: OperationExecution::NotAttempted {
                reason: reason.to_owned(),
            },
        });
    }

    pub(crate) fn finish(self) -> (Vec<OperationTranscript>, Vec<String>) {
        (self.operations, self.redactions)
    }
}

#[derive(Clone)]
pub(crate) enum Fact {
    Pass,
    Fail(String),
    NotEvaluated(String),
}

impl Fact {
    pub(crate) fn from_result(result: std::result::Result<(), String>) -> Self {
        match result {
            Ok(()) => Self::Pass,
            Err(explanation) => Self::Fail(explanation),
        }
    }

    pub(crate) fn from_bool(value: bool, explanation: &str) -> Self {
        if value {
            Self::Pass
        } else {
            Self::Fail(explanation.to_owned())
        }
    }

    pub(crate) fn passed(&self) -> bool {
        matches!(self, Self::Pass)
    }

    pub(crate) fn explanation(&self, fallback: &str) -> String {
        match self {
            Self::Pass => fallback.to_owned(),
            Self::Fail(explanation) | Self::NotEvaluated(explanation) => explanation.clone(),
        }
    }

    pub(crate) fn outcome(&self) -> AssertionOutcome {
        match self {
            Self::Pass => AssertionOutcome::Pass,
            Self::Fail(explanation) => AssertionOutcome::Fail {
                explanation: explanation.clone(),
            },
            Self::NotEvaluated(reason) => AssertionOutcome::NotEvaluated {
                reason: reason.clone(),
            },
        }
    }
}

pub(crate) fn validate_status(
    observation: &Observation,
    allowed: &[u16],
) -> std::result::Result<(), String> {
    match observation.status {
        Some(status) if allowed.contains(&status) => Ok(()),
        Some(status) => Err(format!("HTTP {status} is not in {allowed:?}")),
        None => Err(observation
            .failure
            .as_ref()
            .map(|failure| failure.explanation.clone())
            .unwrap_or_else(|| "no response was received".to_owned())),
    }
}

pub(crate) fn parse_json_response<T: for<'de> Deserialize<'de>>(
    observation: &Observation,
    status: u16,
) -> std::result::Result<T, String> {
    validate_status(observation, &[status])?;
    let value = observation
        .private_json
        .clone()
        .ok_or_else(|| "response did not contain valid JSON".to_owned())?;
    serde_json::from_value(value).map_err(|error| format!("invalid response shape: {error}"))
}

pub(crate) fn validate_error_response(
    observation: &Observation,
    status: u16,
    error_type: &str,
) -> std::result::Result<(), String> {
    let response: IcebergErrorResponse = parse_json_response(observation, status)?;
    if response.error.code != status {
        return Err(format!(
            "error code {} does not match HTTP {status}",
            response.error.code
        ));
    }
    if response.error.r#type != error_type {
        return Err(format!(
            "error type `{}` does not match `{error_type}`",
            response.error.r#type
        ));
    }
    if response.error.message.trim().is_empty() {
        return Err("error message is empty".to_owned());
    }
    Ok(())
}

pub(crate) fn all_results<'a, T: 'a>(
    results: impl IntoIterator<Item = &'a std::result::Result<T, String>>,
) -> std::result::Result<(), String> {
    results
        .into_iter()
        .find_map(|result| result.as_ref().err().cloned())
        .map_or(Ok(()), Err)
}

#[derive(Debug, Deserialize)]
struct IcebergErrorResponse {
    error: IcebergError,
}

#[derive(Debug, Deserialize)]
struct IcebergError {
    message: String,
    r#type: String,
    code: u16,
}
