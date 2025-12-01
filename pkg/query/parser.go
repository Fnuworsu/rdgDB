// Package query - Parser implementation for RQL
package query

import (
	"fmt"
	"strconv"
)

// Parser parses RQL queries into AST
type Parser struct {
	lexer   *Lexer
	current Token
	peek    Token
	errors  []string
}

// NewParser creates a new parser
func NewParser(input string) *Parser {
	l := NewLexer(input)
	p := &Parser{
		lexer:  l,
		errors: []string{},
	}
	// Read two tokens to initialize current and peek
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.current = p.peek
	p.peek = p.lexer.NextToken()
}

func (p *Parser) currentTokenIs(t TokenType) bool {
	return p.current.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peek.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead at line %d",
		t, p.peek.Type, p.peek.Line)
	p.errors = append(p.errors, msg)
}

func (p *Parser) error(msg string) {
	p.errors = append(p.errors, fmt.Sprintf("%s at line %d", msg, p.current.Line))
}

// Errors returns parsing errors
func (p *Parser) Errors() []string {
	return p.errors
}

// Parse parses the entire query
func (p *Parser) Parse() (*Query, error) {
	query := NewQuery()

	// Parse MATCH clause
	if p.currentTokenIs(TokenMatch) {
		match, err := p.parseMatchClause()
		if err != nil {
			return nil, err
		}
		query.Match = match
	}

	// Parse WHERE clause
	if p.currentTokenIs(TokenWhere) {
		where, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}
		query.Where = where
	}

	// Parse RETURN clause
	if p.currentTokenIs(TokenReturn) {
		ret, err := p.parseReturnClause()
		if err != nil {
			return nil, err
		}
		query.Return = ret
	}

	// Parse LIMIT clause
	if p.currentTokenIs(TokenLimit) {
		limit, err := p.parseLimitClause()
		if err != nil {
			return nil, err
		}
		query.Limit = &limit
	}

	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", p.errors)
	}

	return query, nil
}

// parseMatchClause parses MATCH (a)-[]->(b)
func (p *Parser) parseMatchClause() (*MatchClause, error) {
	if !p.currentTokenIs(TokenMatch) {
		return nil, fmt.Errorf("expected MATCH")
	}
	p.nextToken()

	match := &MatchClause{
		Patterns: make([]Pattern, 0),
	}

	// Parse patterns (for now, just one pattern)
	pattern, err := p.parsePattern()
	if err != nil {
		return nil, err
	}
	match.Patterns = append(match.Patterns, *pattern)

	return match, nil
}

// parsePattern parses (a)-[r:TYPE]->(b)
func (p *Parser) parsePattern() (*Pattern, error) {
	pattern := &Pattern{
		Nodes: make([]NodePattern, 0),
		Edges: make([]EdgePattern, 0),
	}

	// Parse first node
	node, err := p.parseNodePattern()
	if err != nil {
		return nil, err
	}
	pattern.Nodes = append(pattern.Nodes, *node)

	// Parse edges and subsequent nodes
	for p.currentTokenIs(TokenDash) || p.currentTokenIs(TokenLeftArrow) {
		edge, err := p.parseEdgePattern()
		if err != nil {
			return nil, err
		}
		pattern.Edges = append(pattern.Edges, *edge)

		// Parse target node
		node, err := p.parseNodePattern()
		if err != nil {
			return nil, err
		}
		pattern.Nodes = append(pattern.Nodes, *node)
	}

	return pattern, nil
}

// parseNodePattern parses (a:Label) or (a:Label {prop: value})
func (p *Parser) parseNodePattern() (*NodePattern, error) {
	if !p.currentTokenIs(TokenLeftParen) {
		return nil, fmt.Errorf("expected ( for node pattern")
	}
	p.nextToken()

	node := &NodePattern{
		Properties: make(map[string]interface{}),
	}

	// Parse variable name (optional)
	if p.currentTokenIs(TokenIdentifier) {
		node.Variable = p.current.Literal
		p.nextToken()
	}

	// Parse label (optional)
	if p.currentTokenIs(TokenColon) {
		p.nextToken()
		if !p.currentTokenIs(TokenIdentifier) {
			return nil, fmt.Errorf("expected label after :")
		}
		node.Label = p.current.Literal
		p.nextToken()
	}

	// Parse inline properties (optional) {name: "Alice"}
	if p.currentTokenIs(TokenLeftBrace) {
		props, err := p.parseProperties()
		if err != nil {
			return nil, err
		}
		node.Properties = props
	}

	if !p.currentTokenIs(TokenRightParen) {
		return nil, fmt.Errorf("expected ) to close node pattern")
	}
	p.nextToken()

	return node, nil
}

// parseEdgePattern parses -[]-> or <-[:TYPE]- or -[]-
func (p *Parser) parseEdgePattern() (*EdgePattern, error) {
	edge := &EdgePattern{}

	// Determine direction by looking at the start
	if p.currentTokenIs(TokenLeftArrow) {
		// <-[...]
		edge.Direction = DirectionIn
		p.nextToken() // consume <-
	} else if p.currentTokenIs(TokenDash) {
		// -[...]
		p.nextToken() // consume -
	} else {
		return nil, fmt.Errorf("expected - or <- to start edge pattern")
	}

	// Parse [...]
	if !p.currentTokenIs(TokenLeftBracket) {
		return nil, fmt.Errorf("expected [ in edge pattern")
	}
	p.nextToken()

	// Parse edge variable and type (optional)
	if p.currentTokenIs(TokenIdentifier) {
		edge.Variable = p.current.Literal
		p.nextToken()
	}

	if p.currentTokenIs(TokenColon) {
		p.nextToken()
		if !p.currentTokenIs(TokenIdentifier) {
			return nil, fmt.Errorf("expected edge type after :")
		}
		edge.Type = p.current.Literal
		p.nextToken()
	}

	if !p.currentTokenIs(TokenRightBracket) {
		return nil, fmt.Errorf("expected ] to close edge pattern")
	}
	p.nextToken()

	// Check for outgoing arrow
	if p.currentTokenIs(TokenArrow) {
		// If we already saw <-, this is bidirectional
		if edge.Direction == DirectionIn {
			edge.Direction = DirectionBoth
		} else {
			// Just -, now we see ->
			edge.Direction = DirectionOut
		}
		p.nextToken()
	} else if p.currentTokenIs(TokenDash) {
		// -[]- (bidirectional/undirected)
		if edge.Direction == 0 {
			edge.Direction = DirectionBoth
		}
		p.nextToken()
	} else if edge.Direction == 0 {
		// Just -[] with no arrow, treat as both
		edge.Direction = DirectionBoth
	}

	return edge, nil
}

// parseProperties parses {key: value, ...}
func (p *Parser) parseProperties() (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if !p.currentTokenIs(TokenLeftBrace) {
		return nil, fmt.Errorf("expected {")
	}
	p.nextToken()

	for !p.currentTokenIs(TokenRightBrace) {
		if !p.currentTokenIs(TokenIdentifier) {
			return nil, fmt.Errorf("expected property name")
		}
		key := p.current.Literal
		p.nextToken()

		if !p.currentTokenIs(TokenColon) {
			return nil, fmt.Errorf("expected : after property name")
		}
		p.nextToken()

		valueExpr, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}

		// Extract actual value from Literal expression
		if lit, ok := valueExpr.(*Literal); ok {
			props[key] = lit.Value
		} else {
			props[key] = valueExpr
		}

		if p.currentTokenIs(TokenComma) {
			p.nextToken()
		} else if !p.currentTokenIs(TokenRightBrace) {
			return nil, fmt.Errorf("expected , or } in properties")
		}
	}

	if !p.currentTokenIs(TokenRightBrace) {
		return nil, fmt.Errorf("expected }")
	}
	p.nextToken()

	return props, nil
}

// parseWhereClause parses WHERE conditions
func (p *Parser) parseWhereClause() (*WhereClause, error) {
	if !p.currentTokenIs(TokenWhere) {
		return nil, fmt.Errorf("expected WHERE")
	}
	p.nextToken()

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	return &WhereClause{Expr: expr}, nil
}

// parseExpression parses expressions with precedence
func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOrExpression()
}

func (p *Parser) parseOrExpression() (Expression, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.currentTokenIs(TokenOr) {
		op := p.current.Literal
		p.nextToken()
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseAndExpression() (Expression, error) {
	left, err := p.parseComparisonExpression()
	if err != nil {
		return nil, err
	}

	for p.currentTokenIs(TokenAnd) {
		op := p.current.Literal
		p.nextToken()
		right, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: op, Right: right}
	}

	return left, nil
}

func (p *Parser) parseComparisonExpression() (Expression, error) {
	left, err := p.parsePrimaryExpression()
	if err != nil {
		return nil, err
	}

	if p.currentTokenIs(TokenEqual) || p.currentTokenIs(TokenNotEqual) ||
		p.currentTokenIs(TokenLess) || p.currentTokenIs(TokenLessEqual) ||
		p.currentTokenIs(TokenGreater) || p.currentTokenIs(TokenGreaterEqual) {
		op := p.current.Literal
		p.nextToken()
		right, err := p.parsePrimaryExpression()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Operator: op, Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	// Property access: p.name
	if p.currentTokenIs(TokenIdentifier) && p.peekTokenIs(TokenDot) {
		variable := p.current.Literal
		p.nextToken() // consume identifier
		p.nextToken() // consume dot
		if !p.currentTokenIs(TokenIdentifier) {
			return nil, fmt.Errorf("expected property name after .")
		}
		prop := p.current.Literal
		p.nextToken()
		return &PropertyAccess{Variable: variable, Property: prop}, nil
	}

	// Identifier
	if p.currentTokenIs(TokenIdentifier) {
		id := &Identifier{Name: p.current.Literal}
		p.nextToken()
		return id, nil
	}

	// Literal
	return p.parseLiteral()
}

func (p *Parser) parseLiteral() (Expression, error) {
	if p.currentTokenIs(TokenString) {
		lit := &Literal{Value: p.current.Literal}
		p.nextToken()
		return lit, nil
	}

	if p.currentTokenIs(TokenNumber) {
		// Try int first, then float
		if val, err := strconv.Atoi(p.current.Literal); err == nil {
			lit := &Literal{Value: val}
			p.nextToken()
			return lit, nil
		}
		if val, err := strconv.ParseFloat(p.current.Literal, 64); err == nil {
			lit := &Literal{Value: val}
			p.nextToken()
			return lit, nil
		}
		return nil, fmt.Errorf("invalid number: %s", p.current.Literal)
	}

	if p.currentTokenIs(TokenTrue) {
		lit := &Literal{Value: true}
		p.nextToken()
		return lit, nil
	}

	if p.currentTokenIs(TokenFalse) {
		lit := &Literal{Value: false}
		p.nextToken()
		return lit, nil
	}

	return nil, fmt.Errorf("unexpected token: %s", p.current.Type)
}

// parseReturnClause parses RETURN items
func (p *Parser) parseReturnClause() (*ReturnClause, error) {
	if !p.currentTokenIs(TokenReturn) {
		return nil, fmt.Errorf("expected RETURN")
	}
	p.nextToken()

	ret := &ReturnClause{
		Items: make([]ReturnItem, 0),
	}

	for {
		expr, err := p.parseReturnExpression()
		if err != nil {
			return nil, err
		}

		item := ReturnItem{Expr: expr}
		ret.Items = append(ret.Items, item)

		if !p.currentTokenIs(TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return ret, nil
}

func (p *Parser) parseReturnExpression() (Expression, error) {
	// Property access or identifier
	return p.parsePrimaryExpression()
}

// parseLimitClause parses LIMIT n
func (p *Parser) parseLimitClause() (int, error) {
	if !p.currentTokenIs(TokenLimit) {
		return 0, fmt.Errorf("expected LIMIT")
	}
	p.nextToken()

	if !p.currentTokenIs(TokenNumber) {
		return 0, fmt.Errorf("expected number after LIMIT")
	}

	limit, err := strconv.Atoi(p.current.Literal)
	if err != nil {
		return 0, fmt.Errorf("invalid LIMIT value: %s", p.current.Literal)
	}

	p.nextToken()
	return limit, nil
}
