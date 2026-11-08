use serde_json::Value;

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub columns: Vec<String>, // empty or ["*"] means all
    pub limit: Option<usize>,
    pub where_field: Option<String>,
    pub where_op: Option<String>,
    pub where_val: Option<String>,
}

impl SelectQuery {
    pub fn parse(sql: &str) -> Self {
        let normalized = sql.trim().trim_end_matches(';');
        let upper = normalized.to_ascii_uppercase();

        let select_body = if let Some(from_idx) = upper.find(" FROM ") {
            normalized
                .get(6..from_idx)
                .unwrap_or("*")
                .trim()
                .to_string()
        } else {
            "*".to_string()
        };

        let columns: Vec<String> = select_body
            .split(',')
            .map(|c| {
                let col = c.trim();
                col.strip_prefix("s.")
                    .or_else(|| col.strip_prefix("S."))
                    .unwrap_or(col)
                    .trim_matches('"')
                    .trim_matches('`')
                    .to_string()
            })
            .collect();

        let limit = if let Some(lim_idx) = upper.find(" LIMIT ") {
            normalized[lim_idx + 7..]
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
        } else {
            None
        };

        let mut where_field = None;
        let mut where_op = None;
        let mut where_val = None;

        if let Some(where_idx) = upper.find(" WHERE ") {
            let end_idx = upper.find(" LIMIT ").unwrap_or(normalized.len());
            let expr = normalized[where_idx + 7..end_idx].trim();
            for op in ["!=", ">=", "<=", "=", ">", "<"] {
                if let Some((lhs, rhs)) = expr.split_once(op) {
                    let field = lhs
                        .trim()
                        .strip_prefix("s.")
                        .or_else(|| lhs.trim().strip_prefix("S."))
                        .unwrap_or(lhs.trim())
                        .trim_matches('"');
                    let val = rhs.trim().trim_matches('\'').trim_matches('"');
                    where_field = Some(field.to_string());
                    where_op = Some(op.to_string());
                    where_val = Some(val.to_string());
                    break;
                }
            }
        }

        Self {
            columns,
            limit,
            where_field,
            where_op,
            where_val,
        }
    }

    pub fn execute_on_bytes(
        &self,
        data: &[u8],
        input_format: &str,
        output_format: &str,
    ) -> Vec<String> {
        let text = String::from_utf8_lossy(data);
        let is_csv = input_format.eq_ignore_ascii_case("csv");
        let out_csv = output_format.eq_ignore_ascii_case("csv");

        let mut results = Vec::new();
        if is_csv {
            let mut lines = text.lines().filter(|l| !l.trim().is_empty());
            let Some(header_line) = lines.next() else {
                return results;
            };
            let headers: Vec<&str> = header_line.split(',').map(|s| s.trim()).collect();
            for line in lines {
                let vals: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
                let mut obj = serde_json::Map::new();
                for (i, h) in headers.iter().enumerate() {
                    let v = vals.get(i).copied().unwrap_or("");
                    obj.insert((*h).to_string(), parse_scalar(v));
                }
                let val = Value::Object(obj);
                if self.matches_where(&val) {
                    results.push(self.project(&val, out_csv));
                    if let Some(lim) = self.limit {
                        if results.len() >= lim {
                            break;
                        }
                    }
                }
            }
        } else {
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                    if self.matches_where(&val) {
                        results.push(self.project(&val, out_csv));
                        if let Some(lim) = self.limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
            }
        }
        results
    }

    fn matches_where(&self, row: &Value) -> bool {
        let (Some(ref field), Some(ref op), Some(ref target)) =
            (&self.where_field, &self.where_op, &self.where_val)
        else {
            return true;
        };
        let Some(cell) = row.get(field) else {
            return false;
        };

        if let (Some(cell_num), Ok(target_num)) = (cell.as_f64(), target.parse::<f64>()) {
            return match op.as_str() {
                "=" => (cell_num - target_num).abs() < f64::EPSILON,
                "!=" => (cell_num - target_num).abs() >= f64::EPSILON,
                ">" => cell_num > target_num,
                ">=" => cell_num >= target_num,
                "<" => cell_num < target_num,
                "<=" => cell_num <= target_num,
                _ => true,
            };
        }

        let cell_str = match cell {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        match op.as_str() {
            "=" => &cell_str == target,
            "!=" => &cell_str != target,
            ">" => &cell_str > target,
            ">=" => &cell_str >= target,
            "<" => &cell_str < target,
            "<=" => &cell_str <= target,
            _ => true,
        }
    }

    fn project(&self, row: &Value, out_csv: bool) -> String {
        let all_cols =
            self.columns.is_empty() || (self.columns.len() == 1 && self.columns[0] == "*");
        if all_cols {
            if out_csv {
                if let Some(obj) = row.as_object() {
                    return obj
                        .values()
                        .map(val_to_csv_cell)
                        .collect::<Vec<_>>()
                        .join(",");
                }
            }
            return serde_json::to_string(row).unwrap_or_default();
        }

        if out_csv {
            self.columns
                .iter()
                .map(|c| row.get(c).map(val_to_csv_cell).unwrap_or_default())
                .collect::<Vec<_>>()
                .join(",")
        } else {
            let mut out = serde_json::Map::new();
            for c in &self.columns {
                if let Some(v) = row.get(c) {
                    out.insert(c.clone(), v.clone());
                }
            }
            serde_json::to_string(&Value::Object(out)).unwrap_or_default()
        }
    }
}

fn parse_scalar(s: &str) -> Value {
    if let Ok(i) = s.parse::<i64>() {
        Value::Number(i.into())
    } else if let Ok(f) = s.parse::<f64>() {
        serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(s.to_string()))
    } else if s.eq_ignore_ascii_case("true") {
        Value::Bool(true)
    } else if s.eq_ignore_ascii_case("false") {
        Value::Bool(false)
    } else {
        Value::String(s.trim_matches('"').to_string())
    }
}

fn val_to_csv_cell(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_json_and_csv() {
        let q = SelectQuery::parse(
            "SELECT s.name, s.score FROM S3Object s WHERE s.score >= 90 LIMIT 2",
        );
        let data = br#"{"name":"alice","score":95,"role":"eng"}
{"name":"bob","score":82,"role":"ops"}
{"name":"carol","score":99,"role":"research"}"#;
        let rows = q.execute_on_bytes(data, "json", "json");
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains("alice"));
        assert!(rows[1].contains("carol"));
    }
}
