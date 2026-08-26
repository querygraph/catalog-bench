use serde_json::Value;
use url::Url;

pub(crate) struct SanitizedJson {
    pub value: Value,
    pub redactions: Vec<String>,
}

pub(crate) fn sanitize_json(value: Value, sensitive_values: &[String]) -> SanitizedJson {
    let mut value = value;
    let mut redactions = Vec::new();
    sanitize_value(&mut value, "", sensitive_values, &mut redactions);
    redactions.sort();
    redactions.dedup();
    SanitizedJson { value, redactions }
}

pub(crate) fn contains_sensitive_value(bytes: &[u8], sensitive_values: &[String]) -> bool {
    sensitive_values.iter().any(|sensitive| {
        sensitive.len() >= 4
            && bytes
                .windows(sensitive.len())
                .any(|window| window == sensitive.as_bytes())
    })
}

fn sanitize_value(
    value: &mut Value,
    pointer: &str,
    sensitive_values: &[String],
    redactions: &mut Vec<String>,
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_pointer = format!("{pointer}/{}", escape_pointer_segment(key));
                if is_sensitive_key(key) {
                    *child = Value::String("<redacted>".to_owned());
                    redactions.push(child_pointer);
                } else {
                    sanitize_value(child, &child_pointer, sensitive_values, redactions);
                }
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter_mut().enumerate() {
                sanitize_value(
                    child,
                    &format!("{pointer}/{index}"),
                    sensitive_values,
                    redactions,
                );
            }
        }
        Value::String(text) => {
            if contains_sensitive_value(text.as_bytes(), sensitive_values)
                || text
                    .strip_prefix("Bearer ")
                    .is_some_and(|token| !token.is_empty())
            {
                *text = "<redacted>".to_owned();
                redactions.push(pointer.to_owned());
            } else if let Some(redacted) = sanitize_url(text) {
                *text = redacted;
                redactions.push(pointer.to_owned());
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    [
        "password",
        "secret",
        "token",
        "privatekey",
        "accesskey",
        "credential",
        "cookie",
        "authorization",
        "apikey",
        "clientid",
        "bearer",
        "sessionid",
        "signature",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn sanitize_url(text: &str) -> Option<String> {
    let mut url = Url::parse(text).ok()?;
    url.host_str()?;

    let mut changed = false;
    if !url.username().is_empty() || url.password().is_some() {
        let _ = url.set_username("redacted");
        let _ = url.set_password(None);
        changed = true;
    }
    if url.fragment().is_some() {
        url.set_fragment(None);
        changed = true;
    }

    let query = url
        .query_pairs()
        .map(|(key, value)| {
            if is_sensitive_key(&key) {
                changed = true;
                (key.into_owned(), "<redacted>".to_owned())
            } else {
                (key.into_owned(), value.into_owned())
            }
        })
        .collect::<Vec<_>>();
    if changed && url.query().is_some() {
        url.query_pairs_mut().clear().extend_pairs(query);
    }

    changed.then(|| url.to_string())
}

fn escape_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}
