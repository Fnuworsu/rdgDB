// Package query implements the RQL (rdgDB Query Language) parser and execution engine
package query

import (
	"fmt"
	"strings"
)

// TokenType represents the type of token
type TokenType int

const (
	// Special tokens
	TokenEOF TokenType = iota
	TokenIllegal

	// Keywords
	TokenMatch
	TokenWhere
	TokenReturn
	TokenLimit
	TokenOrderBy
	TokenAnd
	TokenOr

	// Identifiers and literals
	TokenIdentifier // variable names, labels
	TokenString     // "string literal"
	TokenNumber     // 123, 45.67
	TokenTrue
	TokenFalse

	// Operators
	TokenEqual        // =
	TokenNotEqual     // !=
	TokenLess         // <
	TokenLessEqual    // <=
	TokenGreater      // >
	TokenGreaterEqual // >=

	// Delimiters
	TokenLeftParen    // (
	TokenRightParen   // )
	TokenLeftBracket  // [
	TokenRightBracket // ]
	TokenLeftBrace    // {
	TokenRightBrace   // }
	TokenComma        // ,
	TokenDot          // .
	TokenColon        // :
	TokenArrow        // ->
	TokenLeftArrow    // <-
	TokenDash         // -
	TokenStar         // *
	TokenDotDot       // ..
)

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

// Lexer tokenizes RQL queries
type Lexer struct {
	input        string
	position     int  // current position in input
	readPosition int  // next position to read
	ch           byte // current char
	line         int
	column       int
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar advances to the next character
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // EOF
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	l.column++

	if l.ch == '\n' {
		l.line++
		l.column = 0
	}
}

// peekChar looks ahead one character without advancing
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	tok.Line = l.line
	tok.Column = l.column

	switch l.ch {
	case '(':
		tok = l.newToken(TokenLeftParen, string(l.ch))
	case ')':
		tok = l.newToken(TokenRightParen, string(l.ch))
	case '[':
		tok = l.newToken(TokenLeftBracket, string(l.ch))
	case ']':
		tok = l.newToken(TokenRightBracket, string(l.ch))
	case '{':
		tok = l.newToken(TokenLeftBrace, string(l.ch))
	case '}':
		tok = l.newToken(TokenRightBrace, string(l.ch))
	case ',':
		tok = l.newToken(TokenComma, string(l.ch))
	case '.':
		if l.peekChar() == '.' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(TokenDotDot, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(TokenDot, string(l.ch))
		}
	case ':':
		tok = l.newToken(TokenColon, string(l.ch))
	case '*':
		tok = l.newToken(TokenStar, string(l.ch))
	case '-':
		if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(TokenArrow, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(TokenDash, string(l.ch))
		}
	case '<':
		if l.peekChar() == '-' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(TokenLeftArrow, string(ch)+string(l.ch))
		} else if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(TokenLessEqual, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(TokenLess, string(l.ch))
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(TokenGreaterEqual, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(TokenGreater, string(l.ch))
		}
	case '=':
		tok = l.newToken(TokenEqual, string(l.ch))
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.newToken(TokenNotEqual, string(ch)+string(l.ch))
		} else {
			tok = l.newToken(TokenIllegal, string(l.ch))
		}
	case '"', '\'':
		tok.Type = TokenString
		tok.Literal = l.readString(l.ch)
	case 0:
		tok.Literal = ""
		tok.Type = TokenEOF
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = lookupKeyword(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			tok.Type = TokenNumber
			tok.Literal = l.readNumber()
			return tok
		} else {
			tok = l.newToken(TokenIllegal, string(l.ch))
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) newToken(tokenType TokenType, literal string) Token {
	return Token{
		Type:    tokenType,
		Literal: literal,
		Line:    l.line,
		Column:  l.column,
	}
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	// Handle decimal numbers
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	return l.input[position:l.position]
}

func (l *Lexer) readString(quote byte) string {
	position := l.position + 1
	for {
		l.readChar()
		if l.ch == quote || l.ch == 0 {
			break
		}
		// Handle escaped quotes
		if l.ch == '\\' && l.peekChar() == quote {
			l.readChar()
		}
	}
	return l.input[position:l.position]
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

var keywords = map[string]TokenType{
	"MATCH":  TokenMatch,
	"WHERE":  TokenWhere,
	"RETURN": TokenReturn,
	"LIMIT":  TokenLimit,
	"ORDER":  TokenOrderBy,
	"BY":     TokenOrderBy, // ORDER BY
	"AND":    TokenAnd,
	"OR":     TokenOr,
	"true":   TokenTrue,
	"false":  TokenFalse,
}

func lookupKeyword(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TokenIdentifier
}

// String returns a string representation of the token type
func (t TokenType) String() string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenIllegal:
		return "ILLEGAL"
	case TokenMatch:
		return "MATCH"
	case TokenWhere:
		return "WHERE"
	case TokenReturn:
		return "RETURN"
	case TokenLimit:
		return "LIMIT"
	case TokenIdentifier:
		return "IDENTIFIER"
	case TokenString:
		return "STRING"
	case TokenNumber:
		return "NUMBER"
	case TokenEqual:
		return "="
	case TokenArrow:
		return "->"
	default:
		return fmt.Sprintf("TokenType(%d)", t)
	}
}
