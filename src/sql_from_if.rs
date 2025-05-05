use std::collections::HashSet;
use std::fmt;

// Error types for the parser
#[derive(Debug)]
pub enum ParseError {
    UnexpectedToken(String),
    UnexpectedEndOfInput,
    UnsupportedFunction(String),
    UnsupportedOperator(String),
    InvalidSyntax(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedToken(token) => write!(f, "Unexpected token: {}", token),
            ParseError::UnexpectedEndOfInput => write!(f, "Unexpected end of input"),
            ParseError::UnsupportedFunction(func) => write!(f, "Unsupported function: {}", func),
            ParseError::UnsupportedOperator(op) => write!(f, "Unsupported operator: {}", op),
            ParseError::InvalidSyntax(msg) => write!(f, "Invalid syntax: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

// Token types
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Identifier(String),
    Number(f64),
    String(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Operator(String),
    Equals,
    NotEquals,
    LessThan,
    LessThanEquals,
    GreaterThan,
    GreaterThanEquals,
    And,
    Or,
    Not,
    Dot,
    EOF,
}

// AST node types
#[derive(Debug, Clone)]
pub enum Expr {
    Identifier(String),
    Number(f64),
    String(String),
    BinaryOp {
        left: Box<Expr>,
        operator: String,
        right: Box<Expr>,
    },
    UnaryOp {
        operator: String,
        operand: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    ArrayAccess {
        array: Box<Expr>,
        index: Box<Expr>,
    },
}

// Lexer to tokenize the input
pub struct Lexer {
    input: Vec<char>,
    position: usize,
    current_char: Option<char>,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        let chars: Vec<char> = input.chars().collect();
        let current_char = chars.get(0).cloned();
        Lexer {
            input: chars,
            position: 0,
            current_char,
        }
    }

    fn advance(&mut self) {
        self.position += 1;
        self.current_char = self.input.get(self.position).cloned();
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.current_char {
            if !c.is_whitespace() {
                break;
            }
            self.advance();
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut result = String::new();
        while let Some(c) = self.current_char {
            if c.is_alphanumeric() || c == '_' {
                result.push(c);
                self.advance();
            } else {
                break;
            }
        }
        result
    }

    fn read_number(&mut self) -> f64 {
        let mut result = String::new();
        
        // Handle negative sign as part of the number
        if self.current_char == Some('-') {
            result.push('-');
            self.advance();
        }
        
        while let Some(c) = self.current_char {
            if c.is_digit(10) || c == '.' {
                result.push(c);
                self.advance();
            } else {
                break;
            }
        }
        result.parse().unwrap_or(0.0)
    }

    fn read_string(&mut self) -> String {
        let mut result = String::new();
        self.advance(); // Skip the opening quote
        while let Some(c) = self.current_char {
            if c == '"' || c == '\'' {
                self.advance(); // Skip the closing quote
                break;
            }
            result.push(c);
            self.advance();
        }
        result
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        match self.current_char {
            None => Token::EOF,
            Some('(') => {
                self.advance();
                Token::LParen
            }
            Some(')') => {
                self.advance();
                Token::RParen
            }
            Some('[') => {
                self.advance();
                Token::LBracket
            }
            Some(']') => {
                self.advance();
                Token::RBracket
            }
            Some(',') => {
                self.advance();
                Token::Comma
            }
            Some('.') => {
                self.advance();
                Token::Dot
            }
            Some('=') => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::Equals
                } else {
                    Token::Equals
                }
            }
            Some('!') => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::NotEquals
                } else {
                    Token::Not
                }
            }
            Some('<') => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::LessThanEquals
                } else {
                    Token::LessThan
                }
            }
            Some('>') => {
                self.advance();
                if self.current_char == Some('=') {
                    self.advance();
                    Token::GreaterThanEquals
                } else {
                    Token::GreaterThan
                }
            }
            Some('&') => {
                self.advance();
                if self.current_char == Some('&') {
                    self.advance();
                }
                Token::And
            }
            Some('|') => {
                self.advance();
                if self.current_char == Some('|') {
                    self.advance();
                }
                Token::Or
            }
            Some('"') | Some('\'') => Token::String(self.read_string()),
            Some(c) if c.is_digit(10) => Token::Number(self.read_number()),
            Some(c) if c.is_alphabetic() || c == '_' => {
                let ident = self.read_identifier();
                Token::Identifier(ident)
            },
            Some('-') => {
                // Check if this might be a negative number (if followed by a digit)
                let next_char = self.input.get(self.position + 1).cloned();
                if let Some(next) = next_char {
                    if next.is_digit(10) {
                        // It's a negative number
                        return Token::Number(self.read_number());
                    }
                }
                // Otherwise it's just a minus operator
                self.advance();
                Token::Operator("-".to_string())
            },
            Some(c) => {
                self.advance();
                Token::Operator(c.to_string())
            }
        }
    }
}

// Parser to convert tokens to AST
pub struct Parser {
    lexer: Lexer,
    current_token: Token,
}

impl Parser {
    pub fn new(input: &str) -> Self {
        let mut lexer = Lexer::new(input);
        let current_token = lexer.next_token();
        Parser {
            lexer,
            current_token,
        }
    }

    fn eat_token(&mut self, expected: Token) -> Result<(), ParseError> {
        if std::mem::discriminant(&self.current_token) == std::mem::discriminant(&expected) {
            self.current_token = self.lexer.next_token();
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token)))
        }
    }

    pub fn parse(&mut self) -> Result<Expr, ParseError> {
        self.parse_expression()
    }

    fn parse_expression(&mut self) -> Result<Expr, ParseError> {
        self.parse_logical_or()
    }

    fn parse_logical_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_logical_and()?;

        while self.current_token == Token::Or {
            self.eat_token(Token::Or)?;
            let right = self.parse_logical_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                operator: "OR".to_string(),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_logical_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;

        while self.current_token == Token::And {
            self.eat_token(Token::And)?;
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                operator: "AND".to_string(),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_term()?;

        let operator = match self.current_token {
            Token::Equals => "=",
            Token::NotEquals => "!=",
            Token::LessThan => "<",
            Token::LessThanEquals => "<=",
            Token::GreaterThan => ">",
            Token::GreaterThanEquals => ">=",
            _ => return Ok(left),
        };

        self.current_token = self.lexer.next_token();
        let right = self.parse_term()?;

        Ok(Expr::BinaryOp {
            left: Box::new(left),
            operator: operator.to_string(),
            right: Box::new(right),
        })
    }

    fn parse_term(&mut self) -> Result<Expr, ParseError> {
        match &self.current_token {
            Token::Operator(op) if op == "-" => {
                self.eat_token(Token::Operator("-".to_string()))?;
                // Parse the next term as the operand
                let expr = self.parse_term()?;
                
                // If it's a number, convert it to a negative number directly
                if let Expr::Number(n) = expr {
                    return Ok(Expr::Number(-n));
                } else {
                    // Otherwise, handle it as a unary operation
                    return Ok(Expr::UnaryOp {
                        operator: "-".to_string(),
                        operand: Box::new(expr),
                    });
                }
            },
            Token::Not => {
                self.eat_token(Token::Not)?;
                let operand = self.parse_term()?;
                Ok(Expr::UnaryOp {
                    operator: "NOT".to_string(),
                    operand: Box::new(operand),
                })
            }
            Token::LParen => {
                self.eat_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.eat_token(Token::RParen)?;
                Ok(expr)
            }
            Token::Number(n) => {
                let value = *n;
                self.eat_token(Token::Number(value))?;
                Ok(Expr::Number(value))
            }
            Token::String(s) => {
                let value = s.clone();
                self.eat_token(Token::String(value.clone()))?;
                Ok(Expr::String(value))
            }
            Token::Identifier(name) => {
                let name = name.clone();
                self.eat_token(Token::Identifier(name.clone()))?;

                // Check for function call
                if self.current_token == Token::LParen {
                    self.eat_token(Token::LParen)?;
                    let mut args = Vec::new();

                    if self.current_token != Token::RParen {
                        args.push(self.parse_expression()?);

                        while self.current_token == Token::Comma {
                            self.eat_token(Token::Comma)?;
                            args.push(self.parse_expression()?);
                        }
                    }

                    self.eat_token(Token::RParen)?;
                    return Ok(Expr::FunctionCall { name, args });
                }

                // Check for array access (for Stata variables like x[_n-1])
                if self.current_token == Token::LBracket {
                    self.eat_token(Token::LBracket)?;
                    let index = self.parse_expression()?;
                    self.eat_token(Token::RBracket)?;
                    return Ok(Expr::ArrayAccess {
                        array: Box::new(Expr::Identifier(name)),
                        index: Box::new(index),
                    });
                }

                Ok(Expr::Identifier(name))
            }
            _ => Err(ParseError::UnexpectedToken(format!("{:?}", self.current_token))),
        }
    }
}

// Converter to transform Stata AST to SQL
pub struct StataToSqlConverter {
    supported_functions: HashSet<String>,
}

impl StataToSqlConverter {
    pub fn new() -> Self {
        let mut supported_functions = HashSet::new();
        supported_functions.insert("missing".to_string());
        supported_functions.insert("inrange".to_string());
        supported_functions.insert("inlist".to_string());
        supported_functions.insert("mod".to_string());
        supported_functions.insert("ceil".to_string());
        supported_functions.insert("floor".to_string());
        supported_functions.insert("round".to_string());
        supported_functions.insert("real".to_string());
        supported_functions.insert("string".to_string());

        StataToSqlConverter {
            supported_functions,
        }
    }

    pub fn convert(&self, expr: &Expr) -> Result<String, ParseError> {
        match expr {
            Expr::Identifier(name) => Ok(name.clone()),
            Expr::Number(n) => Ok(n.to_string()),
            Expr::String(s) => Ok(format!("'{}'", s.replace("'", "''"))),
            Expr::BinaryOp { left, operator, right } => {
                let left_sql = self.convert(left)?;
                let right_sql = self.convert(right)?;
                let sql_operator = match operator.as_str() {
                    "=" => "=",
                    "!=" => "!=",
                    "<" => "<",
                    "<=" => "<=",
                    ">" => ">",
                    ">=" => ">=",
                    "AND" => "AND",
                    "OR" => "OR",
                    _ => return Err(ParseError::UnsupportedOperator(operator.clone())),
                };
                Ok(format!("({} {} {})", left_sql, sql_operator, right_sql))
            }
            Expr::UnaryOp { operator, operand } => {
                let operand_sql = self.convert(operand)?;
                match operator.as_str() {
                    "NOT" => Ok(format!("NOT ({})", operand_sql)),
                    _ => Err(ParseError::UnsupportedOperator(operator.clone())),
                }
            }
            Expr::FunctionCall { name, args } => {
                self.convert_function(name, args)
            }
            Expr::ArrayAccess { array, index } => {
                // Stata array access typically refers to variables at different observations
                // This would require special handling in SQL depending on the context
                let array_sql = self.convert(array)?;
                let index_sql = self.convert(index)?;
                // This is a simplified conversion that might need to be adapted based on SQL dialect
                Ok(format!("LAG({}, {})", array_sql, index_sql))
            }
        }
    }

    fn convert_function(&self, name: &str, args: &[Expr]) -> Result<String, ParseError> {
        if !self.supported_functions.contains(name) {
            return Err(ParseError::UnsupportedFunction(name.to_string()));
        }

        match name {
            "missing" => {
                if args.len() != 1 {
                    return Err(ParseError::InvalidSyntax("missing() requires 1 argument".to_string()));
                }
                let arg_sql = self.convert(&args[0])?;
                Ok(format!("({} IS NULL)", arg_sql))
            }
            "inrange" => {
                if args.len() != 3 {
                    return Err(ParseError::InvalidSyntax("inrange() requires 3 arguments".to_string()));
                }
                let value_sql = self.convert(&args[0])?;
                let min_sql = self.convert(&args[1])?;
                let max_sql = self.convert(&args[2])?;
                Ok(format!("({} BETWEEN {} AND {})", value_sql, min_sql, max_sql))
            }
            "inlist" => {
                if args.len() < 2 {
                    return Err(ParseError::InvalidSyntax("inlist() requires at least 2 arguments".to_string()));
                }
                let value_sql = self.convert(&args[0])?;
                let list_items: Result<Vec<String>, _> = args[1..].iter().map(|arg| self.convert(arg)).collect();
                let list_sql = list_items?.join(", ");
                Ok(format!("({} IN ({}))", value_sql, list_sql))
            }
            "mod" => {
                if args.len() != 2 {
                    return Err(ParseError::InvalidSyntax("mod() requires 2 arguments".to_string()));
                }
                let num_sql = self.convert(&args[0])?;
                let div_sql = self.convert(&args[1])?;
                Ok(format!("({} % {})", num_sql, div_sql))
            }
            "ceil" => {
                if args.len() != 1 {
                    return Err(ParseError::InvalidSyntax("ceil() requires 1 argument".to_string()));
                }
                let arg_sql = self.convert(&args[0])?;
                Ok(format!("CEILING({})", arg_sql))
            }
            "floor" => {
                if args.len() != 1 {
                    return Err(ParseError::InvalidSyntax("floor() requires 1 argument".to_string()));
                }
                let arg_sql = self.convert(&args[0])?;
                Ok(format!("FLOOR({})", arg_sql))
            }
            "round" => {
                if args.len() != 1 {
                    return Err(ParseError::InvalidSyntax("round() requires 1 argument".to_string()));
                }
                let arg_sql = self.convert(&args[0])?;
                Ok(format!("ROUND({})", arg_sql))
            }
            "real" => {
                if args.len() != 1 {
                    return Err(ParseError::InvalidSyntax("real() requires 1 argument".to_string()));
                }
                let arg_sql = self.convert(&args[0])?;
                Ok(format!("CAST({} AS REAL)", arg_sql))
            }
            "string" => {
                if args.len() != 1 {
                    return Err(ParseError::InvalidSyntax("string() requires 1 argument".to_string()));
                }
                let arg_sql = self.convert(&args[0])?;
                Ok(format!("CAST({} AS VARCHAR)", arg_sql))
            }
            _ => unreachable!(),
        }
    }
}

// Main function to parse and convert Stata if statement to SQL
pub fn stata_to_sql(input: &str) -> Result<String, ParseError> {
    let mut parser = Parser::new(input);
    let ast = parser.parse()?;
    let converter = StataToSqlConverter::new();
    converter.convert(&ast)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_comparison() {
        let result = stata_to_sql("age > 30").unwrap();
        assert_eq!(result, "(age > 30)");
    }

    #[test]
    fn test_logical_operators() {
        let result = stata_to_sql("age > 30 & gender == \"male\"").unwrap();
        assert_eq!(result, "((age > 30) AND (gender = 'male'))");
    }

    #[test]
    fn test_inrange_function() {
        let result = stata_to_sql("inrange(income, 1000, 5000)").unwrap();
        assert_eq!(result, "(income BETWEEN 1000 AND 5000)");
    }

    #[test]
    fn test_inlist_function() {
        let result = stata_to_sql("inlist(country, \"USA\", \"Canada\", \"Mexico\")").unwrap();
        assert_eq!(result, "(country IN ('USA', 'Canada', 'Mexico'))");
    }

    #[test]
    fn test_missing_function() {
        let result = stata_to_sql("missing(value)").unwrap();
        assert_eq!(result, "(value IS NULL)");
    }

    #[test]
    fn test_complex_expression() {
        let result = stata_to_sql("inrange(age, 18, 65) & !missing(income) | status == \"active\"").unwrap();
        assert_eq!(result, "(((age BETWEEN 18 AND 65) AND NOT ((income IS NULL))) OR (status = 'active'))");
    }

    #[test]
    fn test_unsupported_function() {
        let result = stata_to_sql("regexm(text, \"pattern\")");
        assert!(result.is_err());
        if let Err(ParseError::UnsupportedFunction(func)) = result {
            assert_eq!(func, "regexm");
        }
    }
}
