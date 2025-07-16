use regex::Regex;

pub struct StataToSqlRegexConverter {
    replacements: Vec<(Regex, String)>,
}

impl StataToSqlRegexConverter {
    pub fn new() -> Self {
        let mut converter = StataToSqlRegexConverter {
            replacements: Vec::new(),
        };
        
        // Add only Stata-specific patterns - leave SQL syntax alone
        converter.add_patterns();
        converter
    }
    
    fn add_patterns(&mut self) {
        // Handle !missing(var) => var IS NOT NULL
        self.add_replacement(
            r"!missing\s*\(\s*([^)]+)\s*\)",
            "$1 IS NOT NULL"
        );
        
        // Handle missing(var) => var IS NULL
        self.add_replacement(
            r"missing\s*\(\s*([^)]+)\s*\)",
            "$1 IS NULL"
        );
        
        // Handle inrange(var, min, max) => var BETWEEN min AND max
        self.add_replacement(
            r"inrange\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)",
            "$1 BETWEEN $2 AND $3"
        );
        
        // Handle inlist(var, val1, val2, ...) => var IN (val1, val2, ...)
        self.add_replacement(
            r"inlist\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)",
            "$1 IN ($2)"
        );
        
        // Handle mod(a, b) => a % b
        self.add_replacement(
            r"mod\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)",
            "($1 % $2)"
        );
        
        // Handle ceil(x) => CEILING(x)
        self.add_replacement(
            r"ceil\s*\(\s*([^)]+)\s*\)",
            "CEILING($1)"
        );
        
        // Handle floor(x) => FLOOR(x)
        self.add_replacement(
            r"floor\s*\(\s*([^)]+)\s*\)",
            "FLOOR($1)"
        );
        
        // Handle round(x) => ROUND(x)
        self.add_replacement(
            r"round\s*\(\s*([^)]+)\s*\)",
            "ROUND($1)"
        );
        
        // Handle real(x) => CAST(x AS REAL)
        self.add_replacement(
            r"real\s*\(\s*([^)]+)\s*\)",
            "CAST($1 AS REAL)"
        );
        
        // Handle string(x) => CAST(x AS VARCHAR)
        self.add_replacement(
            r"string\s*\(\s*([^)]+)\s*\)",
            "CAST($1 AS VARCHAR)"
        );
        
        // Handle Stata logical operators (simple patterns since we handle quotes separately)
        self.add_replacement(r"\s*&\s*", " AND ");
        self.add_replacement(r"\s*\|\s*", " OR ");
        self.add_replacement(r"==", "=");
        
        // Handle negation with parentheses
        self.add_replacement(r"!\s*\(", "NOT (");
    }
    
    fn add_replacement(&mut self, pattern: &str, replacement: &str) {
        let regex = Regex::new(pattern).expect("Invalid regex pattern");
        self.replacements.push((regex, replacement.to_string()));
    }
    
    pub fn convert(&self, input: &str) -> String {
        // Split input into parts: quoted strings and non-quoted parts
        let parts = self.split_preserving_quotes(input);
        
        let mut result = String::new();
        for (content, is_quoted) in parts {
            if is_quoted {
                // Keep quoted content unchanged
                result.push_str(&content);
            } else {
                // Apply replacements to non-quoted content
                let mut processed = content;
                for (regex, replacement) in &self.replacements {
                    processed = regex.replace_all(&processed, replacement.as_str()).to_string();
                }
                result.push_str(&processed);
            }
        }
        
        result.replace('"', "'")
    }
    

    fn split_preserving_quotes(&self, input: &str) -> Vec<(String, bool)> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_quote = false;
        let mut quote_char = None;
        let mut chars = input.chars().peekable();
        
        while let Some(ch) = chars.next() {
            match ch {
                '"' | '\'' if !in_quote => {
                    // Starting a quoted section
                    if !current.is_empty() {
                        parts.push((current.clone(), false));
                        current.clear();
                    }
                    current.push(ch);
                    in_quote = true;
                    quote_char = Some(ch);
                }
                ch if in_quote && Some(ch) == quote_char => {
                    // Ending a quoted section
                    current.push(ch);
                    parts.push((current.clone(), true));
                    current.clear();
                    in_quote = false;
                    quote_char = None;
                }
                _ => {
                    current.push(ch);
                }
            }
        }
        
        // Add any remaining content
        if !current.is_empty() {
            parts.push((current, in_quote));
        }
        
        parts
    }
}


pub fn stata_to_sql(input: &str) -> String {
    let converter = StataToSqlRegexConverter::new();
    converter.convert(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stata_functions_converted() {
        assert_eq!(stata_to_sql("missing(age)"), "age IS NULL");
        assert_eq!(stata_to_sql("!missing(age)"), "age IS NOT NULL");
        assert_eq!(stata_to_sql("inrange(age, 18, 65)"), "age BETWEEN 18 AND 65");
        assert_eq!(stata_to_sql("inlist(country, \"USA\", \"Canada\")"), "country IN (\"USA\", \"Canada\")");
        assert_eq!(stata_to_sql("ceil(value)"), "CEILING(value)");
        assert_eq!(stata_to_sql("mod(x, 5)"), "(x % 5)");
    }

    #[test]
    fn test_stata_operators_converted() {
        assert_eq!(stata_to_sql("age > 30 & gender == \"male\""), "age > 30 AND gender = \"male\"");
        assert_eq!(stata_to_sql("status == 1 | status == 2"), "status = 1 OR status = 2");
    }

    #[test]
    fn test_sql_syntax_unchanged() {
        // Standard SQL should pass through unchanged
        assert_eq!(stata_to_sql("age > 30 AND gender = 'male'"), "age > 30 AND gender = 'male'");
        assert_eq!(stata_to_sql("status = 1 OR status = 2"), "status = 1 OR status = 2");
        assert_eq!(stata_to_sql("value BETWEEN 1 AND 10"), "value BETWEEN 1 AND 10");
        assert_eq!(stata_to_sql("country IN ('USA', 'Canada')"), "country IN ('USA', 'Canada')");
        assert_eq!(stata_to_sql("name IS NULL"), "name IS NULL");
        assert_eq!(stata_to_sql("name IS NOT NULL"), "name IS NOT NULL");
        assert_eq!(stata_to_sql("CEILING(value)"), "CEILING(value)");
        assert_eq!(stata_to_sql("FLOOR(value)"), "FLOOR(value)");
        assert_eq!(stata_to_sql("CAST(x AS INTEGER)"), "CAST(x AS INTEGER)");
    }

    #[test]
    fn test_mixed_syntax() {
        // Mix of Stata and SQL should convert only Stata parts
        assert_eq!(
            stata_to_sql("inrange(age, 18, 65) AND status = 'active'"),
            "age BETWEEN 18 AND 65 AND status = 'active'"
        );
        assert_eq!(
            stata_to_sql("missing(income) | salary IS NOT NULL"),
            "income IS NULL OR salary IS NOT NULL"
        );
    }

    #[test]
    fn test_complex_mixed_expression() {
        let input = "inrange(age, 18, 65) & !missing(income) | status = 'exempt' AND CEILING(score) > 80";
        let expected = "age BETWEEN 18 AND 65 AND income IS NOT NULL OR status = 'exempt' AND CEILING(score) > 80";
        assert_eq!(stata_to_sql(input), expected);
    }

    #[test]
    fn test_operators_in_strings_unchanged() {
        // Operators inside strings should not be converted
        assert_eq!(stata_to_sql("name = 'John & Jane'"), "name = 'John & Jane'");
        assert_eq!(stata_to_sql("text = \"a | b\""), "text = \"a | b\"");
        assert_eq!(stata_to_sql("value = 'x == y'"), "value = 'x == y'");
        assert_eq!(stata_to_sql("desc = \"missing(data)\""), "desc = \"missing(data)\"");
    }

    #[test]
    fn test_operator_spacing_variations() {
        assert_eq!(stata_to_sql("a&b"), "a AND b");
        assert_eq!(stata_to_sql("a & b"), "a AND b");
        assert_eq!(stata_to_sql("a  &  b"), "a AND b");
        assert_eq!(stata_to_sql("a|b"), "a OR b");
        assert_eq!(stata_to_sql("a | b"), "a OR b");
        assert_eq!(stata_to_sql("a  |  b"), "a OR b");
    }

    #[test]
    fn test_mixed_quotes_and_operators() {
        assert_eq!(
            stata_to_sql("name = 'John & Jane' & age > 30"),
            "name = 'John & Jane' AND age > 30"
        );
        assert_eq!(
            stata_to_sql("missing(name) | desc = \"has | in text\""),
            "name IS NULL OR desc = \"has | in text\""
        );
    }
}

