use chrono::Local;

pub(super) fn append_environment_context(existing: Option<String>, workspace: &str) -> String {
    let context = format_environment_context(workspace, std::env::consts::OS, &Local::now().date_naive().to_string());

    match existing.filter(|value| !value.trim().is_empty()) {
        Some(existing) => format!("{existing}\n\n{context}"),
        None => context,
    }
}

fn format_environment_context(workspace: &str, operating_system: &str, current_date: &str) -> String {
    format!(
        "<environment_context>\n  <operating_system>{}</operating_system>\n  <current_date>{}</current_date>\n  <current_working_directory>{}</current_working_directory>\n</environment_context>",
        escape_xml(operating_system),
        escape_xml(current_date),
        escape_xml(workspace),
    )
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_environment_context() {
        assert_eq!(
            format_environment_context("/workspace/project", "linux", "2026-07-16"),
            "<environment_context>\n  <operating_system>linux</operating_system>\n  <current_date>2026-07-16</current_date>\n  <current_working_directory>/workspace/project</current_working_directory>\n</environment_context>"
        );
    }

    #[test]
    fn appends_context_after_existing_rules_and_escapes_workspace() {
        let result = append_environment_context(Some("Be concise.".to_owned()), "/workspace/A&B <draft>");

        assert!(result.starts_with("Be concise.\n\n<environment_context>"));
        assert!(
            result.contains("<current_working_directory>/workspace/A&amp;B &lt;draft&gt;</current_working_directory>")
        );
    }
}
