use std::str::SplitWhitespace;

#[derive(Debug)]
pub enum Token<'a> {
    None,
    Meta(&'a str),
    Other(&'a str),
}

impl<'a> From<&'a str> for Token<'a> {
    fn from(s: &'a str) -> Self {
        match s.chars().nth(0) {
            None => Self::None,
            Some('.') => Self::Meta(s),
            Some(_) => Self::Other(s),
        }
    }
}

impl<'a> From<Token<'a>> for &'a str {
    fn from(token: Token<'a>) -> Self {
        match token {
            Token::None => "",
            Token::Meta(s) | Token::Other(s) => s,
        }
    }
}

impl<'a> From<Token<'a>> for String {
    fn from(token: Token<'a>) -> Self {
        let s: &'a str = token.into();
        String::from(s)
    }
}

#[derive(Debug)]
pub struct Tokens<'a> {
    raw: SplitWhitespace<'a>,
    next: Option<Token<'a>>,
}

impl<'a> Tokens<'a> {
    pub fn peek(&mut self) -> Option<&<Self as Iterator>::Item> {
        self.next.as_ref()
    }
}

impl<'a> Iterator for Tokens<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let last = self.next.take();
        self.next = self.raw.next().map(|x| x.into());
        last
    }
}

impl<'a> From<Tokens<'a>> for String {
    fn from(tokens: Tokens) -> Self {
        let tokens: Vec<_> = tokens.map(String::from).collect();
        tokens.join(" ")
    }
}

impl<'a> From<&'a str> for Tokens<'a> {
    fn from(raw: &'a str) -> Self {
        let mut raw = raw.trim().split_whitespace();
        let next = raw.next().map(|x| x.into());
        Self { raw, next }
    }
}
