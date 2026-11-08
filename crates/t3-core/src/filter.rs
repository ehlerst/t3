use regex::{Regex, RegexSet};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FilterError {
    #[error("Invalid glob pattern '{0}': {1}")]
    InvalidGlob(String, String),
}

/// High-speed DFA glob matcher using compiled `RegexSet`s.
/// Follows `s5cmd` semantics:
/// - If `--include` patterns are specified, a candidate must match at least one `--include` pattern.
/// - If `--exclude` patterns are specified, any candidate matching an `--exclude` pattern is rejected
///   (`--exclude` always takes precedence over `--include`, fixing s5cmd issues #815 and #852).
#[derive(Debug, Clone, Default)]
pub struct GlobFilter {
    exact_glob: Option<Regex>,
    include_set: Option<RegexSet>,
    exclude_set: Option<RegexSet>,
}

impl GlobFilter {
    pub fn compile(
        key_glob: Option<&str>,
        includes: &[String],
        excludes: &[String],
    ) -> Result<Self, FilterError> {
        let exact_glob = match key_glob {
            Some(g) => {
                let re_str = glob_to_regex(g, true);
                let re = Regex::new(&re_str)
                    .map_err(|e| FilterError::InvalidGlob(g.to_string(), e.to_string()))?;
                Some(re)
            }
            None => None,
        };

        let include_set = if includes.is_empty() {
            None
        } else {
            let patterns: Vec<String> = includes.iter().map(|g| glob_to_regex(g, false)).collect();
            Some(
                RegexSet::new(&patterns)
                    .map_err(|e| FilterError::InvalidGlob(includes.join(","), e.to_string()))?,
            )
        };

        let exclude_set = if excludes.is_empty() {
            None
        } else {
            let patterns: Vec<String> = excludes.iter().map(|g| glob_to_regex(g, false)).collect();
            Some(
                RegexSet::new(&patterns)
                    .map_err(|e| FilterError::InvalidGlob(excludes.join(","), e.to_string()))?,
            )
        };

        Ok(Self {
            exact_glob,
            include_set,
            exclude_set,
        })
    }

    #[inline]
    pub fn matches(&self, full_key: &str, rel_key: &str) -> bool {
        if let Some(ref eg) = self.exact_glob {
            if !eg.is_match(full_key) {
                return false;
            }
        }
        if let Some(ref exc) = self.exclude_set {
            if exc.is_match(rel_key) || exc.is_match(full_key) {
                return false;
            }
        }
        if let Some(ref inc) = self.include_set {
            if !inc.is_match(rel_key) && !inc.is_match(full_key) {
                return false;
            }
        }
        true
    }
}

/// Converts a shell/s5cmd glob pattern into a regular expression.
/// In `s5cmd`, `*` matches any sequence of characters (including `/` when used in `--include`/`--exclude` or trailing `*`).
pub fn glob_to_regex(glob: &str, anchored_full: bool) -> String {
    let mut re = String::with_capacity(glob.len() * 2 + 8);
    if anchored_full {
        re.push('^');
    } else if !glob.starts_with('*') && !glob.starts_with('/') {
        re.push_str("(?:^|/)");
    } else {
        re.push('^');
    }

    let chars: Vec<char> = glob.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => {
                if i + 1 < chars.len() && chars[i + 1] == '*' {
                    re.push_str(".*");
                    i += 2;
                    continue;
                }
                re.push_str(".*");
            }
            '?' => re.push('.'),
            '[' => {
                re.push('[');
                i += 1;
                if i < chars.len() && (chars[i] == '!' || chars[i] == '^') {
                    re.push('^');
                    i += 1;
                }
                while i < chars.len() && chars[i] != ']' {
                    if chars[i] == '\\' {
                        re.push_str("\\\\");
                    } else {
                        re.push(chars[i]);
                    }
                    i += 1;
                }
                re.push(']');
            }
            '.' | '+' | '(' | ')' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                re.push('\\');
                re.push(chars[i]);
            }
            c => re.push(c),
        }
        i += 1;
    }

    re.push('$');
    re
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_filter_exclude_precedence() {
        let filter = GlobFilter::compile(
            Some("logs/2026/*/*.parquet"),
            &["*.parquet".to_string()],
            &["*secret*".to_string(), "*.tmp.parquet".to_string()],
        )
        .unwrap();

        assert!(filter.matches("logs/2026/01/data.parquet", "01/data.parquet"));
        assert!(!filter.matches("logs/2026/01/secret_data.parquet", "01/secret_data.parquet"));
        assert!(!filter.matches("logs/2026/01/data.tmp.parquet", "01/data.tmp.parquet"));
        assert!(!filter.matches("logs/2026/01/data.csv", "01/data.csv"));
    }
}
