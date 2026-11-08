/// Tokenizer for `s5cmd run` batch script lines.
/// Supports `#` comments, empty lines, single/double quotes, and backslash escapes.
pub fn parse_run_line(line: &str) -> Option<Vec<String>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = trimmed.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '#' && !in_single && !in_double && current.is_empty() {
            break;
        }
        match c {
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            '\\' if !in_single => {
                if let Some(next_c) = chars.next() {
                    current.push(next_c);
                }
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if !current.is_empty() {
                    args.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        args.push(current);
    }

    if args.is_empty() {
        None
    } else {
        Some(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_run_line() {
        assert_eq!(parse_run_line("   # comment line"), None);
        assert_eq!(parse_run_line(""), None);
        assert_eq!(
            parse_run_line("cp 's3://my-bucket/path with spaces/a.txt' ./local/"),
            Some(vec![
                "cp".to_string(),
                "s3://my-bucket/path with spaces/a.txt".to_string(),
                "./local/".to_string()
            ])
        );
    }
}
