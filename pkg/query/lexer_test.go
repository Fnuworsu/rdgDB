package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer_BasicTokens(t *testing.T) {
	input := `MATCH (a)-[]->(b)`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenMatch, "MATCH"},
		{TokenLeftParen, "("},
		{TokenIdentifier, "a"},
		{TokenRightParen, ")"},
		{TokenDash, "-"},
		{TokenLeftBracket, "["},
		{TokenRightBracket, "]"},
		{TokenArrow, "->"},
		{TokenLeftParen, "("},
		{TokenIdentifier, "b"},
		{TokenRightParen, ")"},
		{TokenEOF, ""},
	}

	l := NewLexer(input)

	for i, tt := range tests {
		tok := l.NextToken()
		assert.Equal(t, tt.expectedType, tok.Type, "test %d - tokentype wrong", i)
		assert.Equal(t, tt.expectedLiteral, tok.Literal, "test %d - literal wrong", i)
	}
}

func TestLexer_Keywords(t *testing.T) {
	input := `MATCH WHERE RETURN LIMIT AND OR`

	tests := []TokenType{
		TokenMatch,
		TokenWhere,
		TokenReturn,
		TokenLimit,
		TokenAnd,
		TokenOr,
		TokenEOF,
	}

	l := NewLexer(input)

	for i, tt := range tests {
		tok := l.NextToken()
		assert.Equal(t, tt, tok.Type, "test %d", i)
	}
}

func TestLexer_Operators(t *testing.T) {
	input := `= != < <= > >=`

	tests := []TokenType{
		TokenEqual,
		TokenNotEqual,
		TokenLess,
		TokenLessEqual,
		TokenGreater,
		TokenGreaterEqual,
		TokenEOF,
	}

	l := NewLexer(input)

	for i, tt := range tests {
		tok := l.NextToken()
		assert.Equal(t, tt, tok.Type, "test %d", i)
	}
}

func TestLexer_Strings(t *testing.T) {
	input := `"Alice" 'Bob'`

	l := NewLexer(input)

	tok := l.NextToken()
	assert.Equal(t, TokenString, tok.Type)
	assert.Equal(t, "Alice", tok.Literal)

	tok = l.NextToken()
	assert.Equal(t, TokenString, tok.Type)
	assert.Equal(t, "Bob", tok.Literal)
}

func TestLexer_Numbers(t *testing.T) {
	input := `123 45.67 0`

	l := NewLexer(input)

	tok := l.NextToken()
	assert.Equal(t, TokenNumber, tok.Type)
	assert.Equal(t, "123", tok.Literal)

	tok = l.NextToken()
	assert.Equal(t, TokenNumber, tok.Type)
	assert.Equal(t, "45.67", tok.Literal)

	tok = l.NextToken()
	assert.Equal(t, TokenNumber, tok.Type)
	assert.Equal(t, "0", tok.Literal)
}

func TestLexer_CompleteQuery(t *testing.T) {
	input := `
		MATCH (p:Person)-[:KNOWS]->(friend:Person)
		WHERE p.age > 25 AND friend.city = "SF"
		RETURN p.name, friend.name
		LIMIT 10
	`

	l := NewLexer(input)

	expectedTokens := []TokenType{
		TokenMatch,
		TokenLeftParen,
		TokenIdentifier, // p
		TokenColon,
		TokenIdentifier, // Person
		TokenRightParen,
		TokenDash,
		TokenLeftBracket,
		TokenColon,
		TokenIdentifier, // KNOWS
		TokenRightBracket,
		TokenArrow,
		TokenLeftParen,
		TokenIdentifier, // friend
		TokenColon,
		TokenIdentifier, // Person
		TokenRightParen,
		TokenWhere,
		TokenIdentifier, // p
		TokenDot,
		TokenIdentifier, // age
		TokenGreater,
		TokenNumber, // 25
		TokenAnd,
		TokenIdentifier, // friend
		TokenDot,
		TokenIdentifier, // city
		TokenEqual,
		TokenString, // "SF"
		TokenReturn,
		TokenIdentifier, // p
		TokenDot,
		TokenIdentifier, // name
		TokenComma,
		TokenIdentifier, // friend
		TokenDot,
		TokenIdentifier, // name
		TokenLimit,
		TokenNumber, // 10
		TokenEOF,
	}

	for i, expected := range expectedTokens {
		tok := l.NextToken()
		assert.Equal(t, expected, tok.Type, "token %d mismatch", i)
	}
}

func TestLexer_VariableLengthPath(t *testing.T) {
	input := `[*1..3]`

	l := NewLexer(input)

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenLeftBracket, "["},
		{TokenStar, "*"},
		{TokenNumber, "1"},
		{TokenDotDot, ".."},
		{TokenNumber, "3"},
		{TokenRightBracket, "]"},
		{TokenEOF, ""},
	}

	for i, tt := range tests {
		tok := l.NextToken()
		assert.Equal(t, tt.expectedType, tok.Type, "test %d", i)
		assert.Equal(t, tt.expectedLiteral, tok.Literal, "test %d", i)
	}
}

func TestLexer_CaseInsensitiveKeywords(t *testing.T) {
	input := `match Match MATCH where WHERE`

	l := NewLexer(input)

	for i := 0; i < 3; i++ {
		tok := l.NextToken()
		assert.Equal(t, TokenMatch, tok.Type, "MATCH keyword %d", i)
	}

	for i := 0; i < 2; i++ {
		tok := l.NextToken()
		assert.Equal(t, TokenWhere, tok.Type, "WHERE keyword %d", i)
	}
}
