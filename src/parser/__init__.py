from typing import List, Optional, Any

from src.lexer import Token, TokenType, tokenize

from src.parser.ast import (
    Literal, Identifier, Star, BinaryOp, UnaryOp, FunctionCall,
    Alias, InList, IsNull, Between, CaseExpr, Subquery,
    TableRef, JoinClause,
    SelectStatement, ColumnDef, CreateTable, AlterTable,
    InsertInto, UpdateStatement, DeleteStatement,

)


class ParseError(Exception):
    pass


class Parser:
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0
    

    def peek(self) -> Token:                                    # token type return type
        return self.tokens[self.pos]
    
    def peek2(self) -> Token:                                   # return the token ahead of current token's pos
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1]
        return self.tokens[-1]
    

    def advance(self) -> Token:                                 # gets current token and move forward +1
        t = self.tokens[self.pos]
        self.pos += 1
        return t
    
    def at_end(self) -> bool:                                   # checks ends of tokens
        return self.peek().ttype == TokenType.EOF
    
    def check(self, ttype: TokenType, value: str = None) -> bool:
        t = self.peek()
        if t.ttype != ttype:
            return False
        
        if value is not None and t.value.upper() != value.upper():
            return False
        
        return True

    def match(self, ttype: TokenType, *values) -> bool:
        t = self.peek()
        if t.ttype != ttype:
            return False
        
        if values and t.value.upper() not in [v.upper() for v in values]:
            return False

        self.advance()
        return True
        

    
    def expect(self, ttype: TokenType, value: str = None) -> Token:
        t = self.peek()
        if t.ttype != ttype:
            raise ParseError(
                f"Expected {ttype.name} but got {t.ttype.name}({t.value!r}) at pos {t.pos}"
            )
        if value is not None and t.value.upper() != value.upper():
            raise ParseError(
                f"Expected keyword {value!r} but got {t.value!r} at pos {t.pos}"
            )
        return self.advance()


    def parse(self) -> Any:
        stmt = self.parse_statement()
        self.match(TokenType.OP, ";")
        if not self.at_end():
            raise ParseError(f"Unexpect token {self.peek().value!r} after statement")
        return stmt

    def parse_statement(self) -> Any:
        t = self.peek()
        if t.ttype == TokenType.KEYWORD:
            keyword = t.value

            if keyword.upper() == "SELECT":
                return self.parse_select()
            elif keyword.upper() == "CREATE":
                return self.parse_create()
            elif keyword.upper() == "INSERT":
                return self.parse_insert()
            elif keyword.upper() == "UPDATE":
                return self.parse_update()
            elif keyword.upper() == "DELETE":
                return self.parse_delete()
            elif keyword.upper() == "ALTER":
                return self.parse_alter()
            else:
                raise ParseError(f"Unknown keyword {keyword!r}")
        else:
            raise ParseError(f"Expected keyword but got {t.ttype.name}({t.value!r})")
    
    def parse_select(self) -> SelectStatement:
        self.expect(TokenType.KEYWORD, "SELECT")
        distinct = bool(self.match(TokenType.KEYWORD, "DISTINCT"))
        columns = self.parse_select_columns()
        from_table = None
        joins = []
        if self.match(TokenType.KEYWORD, "FROM"):
            from_table = self.parse_table_ref()
            joins = self.parse_joins()      
        
        where = None
        if self.match(TokenType.KEYWORD, "WHERE"):
            where = self.parse_expr()
        
        grup_by = []

        if self.check(TokenType.KEYWORD, "GROUP"):
            self.advance()
            self.expect(TokenType.KEYWORD, "BY")

            grup_by = self.parse_expr_list()
        
        having = None
        if self.match(TokenType.KEYWORD, "HAVING"):
            having = self.parse_expr()
        
        order_by = []
        if self.check(TokenType.KEYWORD, "ORDER"):
            self.advance()
            self.expect(TokenType.KEYWORD, "BY")
            order_by = self.parse_order_list()
        
        limit = None
        if self.match(TokenType.KEYWORD, "LIMIT"):
            limit = int(self.expect(TokenType.NUMBER).value)
        
        offset = None
        if self.match(TokenType.KEYWORD, "OFFSET"):
            offset = int(self.expect(TokenType.NUMBER).value)
        
        return SelectStatement(
            distinct=distinct,
            columns=columns,
            from_table=from_table,
            joins=joins,
            where=where,
            group_by=grup_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
        
    def parse_select_columns(self) -> List[Any]:
        cols = [self.parse_select_columns()]

        while self.match(TokenType.OP, ","):
            cols.append(self.parse_select_columns())
        
        return cols
    
    def parse_select_column(self) -> Any:
        if self.check(TokenType.OP, "*"):
            self.advance()
            return Star()
        
        expr = self.parse_expr()

        if self.match(TokenType.KEYWORD,"AS"):
            alias = self.expect(TokenType.IDENTIFIER).value
            return Alias(expr, alias)
        return expr
        
    
    def parse_table_ref(self) -> TableRef:
        name = self.expect(TokenType.IDENTIFIER).value
        alias = None
        if self.match(TokenType.KEYWORD, "AS"):
            alias = self.expect(TokenType.IDENTIFIER).value
        elif self.check(TokenType.IDENTIFIER):
            alias = self.advance().value
        
        return TableRef(name, alias)
    

    def parse_joins(self) -> List[JoinClause]:
        joins = []

        while True:
            join_type = None

            if self.match(TokenType.KEYWORD, "JOIN"):
                join_type = "INNER"
            elif self.match(TokenType.KEYWORD, "INNER"):
                self.expect(TokenType.KEYWORD, "JOIN")
                
                join_type = "INNER"

            elif self.match(TokenType.KEYWORD, "LEFT"):
                self.match(TokenType.KEYWORD, "OUTER")
                self.expect(TokenType.KEYWORD, "JOIN")
                join_type = "LEFT"
            
            elif self.match(TokenType.KEYWORD, "RIGHT"):
                self.match(TokenType.KEYWORD, "OUTER")
                self.expect(TokenType.KEYWORD, "JOIN")
                join_type = "RIGHT"
            
            elif self.match(TokenType.KEYWORD, "FULL"):
                self.match(TokenType.KEYWORD, "OUTER")
                self.expect(TokenType.KEYWORD, "JOIN")
                join_type = "FULL"
            
            elif self.match(TokenType.KEYWORD, "CROSS"):
                self.expect(TokenType.KEYWORD, "JOIN")
                join_type = "CROSS"
            
            else:
                break
            
            table = self.parse_table_ref()
            self.expect(TokenType.KEYWORD, "ON")
            on = self.parse_expr()
            joins.append(JoinClause(join_type, table, on))
        
        return joins
    
    def parse_order_list(self) -> Any:

        items = []
        expr = self.parse_expr()
        direction = "ASC"
        
        if self.match(TokenType.KEYWORD, "DESC"):
            direction = "DESC"
        else:
            self.match(TokenType.KEYWORD, "ASC")
        
        items.append((expr, direction))

        while self.match(TokenType.OP, ","):
            expr = self.parse_expr()
            direction = "ASC"
            if self.match(TokenType.KEYWORD, "DESC"):
                direction = "DESC"
            else:
                self.match(TokenType.KEYWORD, "ASC")
            
            items.append((expr, direction))
        
        return items
    
    def parse_expr_list(self) -> List[Any]:
        exprs = [self.parse_expr()]
        while self.match(TokenType.OP, ","):
            exprs.append(self.parse_expr())
        return exprs
    

    def parse_expr(self) -> Any:
        return self.parse_or()

    def parse_or(self) -> Any:
        left = self.parse_and()
        while self.match(TokenType.KEYWORD, "OR"):
            right = self.parse_and()
            left = BinaryOp("OR", left, right)
        return left
    
    def parse_and(self) -> Any:
        left = self.parse_not()
        while self.match(TokenType.KEYWORD, "AND"):
            right = self.parse_not()
            left = BinaryOp("AND", left, right)
        return left

    def parse_not(self) -> Any:
        if self.match(TokenType.KEYWORD, "NOT"):
            return UnaryOp("NOT", self.parse_not())
        return self.parse_compare()
    
    def parse_compare(self) -> Any:
        left = self.parse_and()
        t = self.peek()

        if t.ttype == TokenType.OP and t.value in ("=", "!=", "<>", "<", "<=", ">", ">="):
            op = self.advance().value
            if op == "<>": op = "!="
            return BinaryOp(op, left, self.parse_add())
        
        if t.ttype == TokenType.KEYWORD and t.value == "LIKE":
            self.advance()
            return BinaryOp("LIKE", left, self.parse_add())

        if t.ttype == TokenType.KEYWORD and t.value == "IS":
            self.advance()
            negated = bool(self.match(TokenType.KEYWORD, "NOT"))
            self.expect(TokenType.KEYWORD, "NULL")
            return IsNull(left, negated)
        
        if t.ttype == TokenType.KEYWORD and t.value == "BETWEEN":
            self.advance()
            low = self.parse_add()
            self.expect(TokenType.KEYWORD, "AND")
            high = self.parse_add()
            return Between(left, low, high, negated=False)

        if t.ttype == TokenType.KEYWORD and t.value == "NOT":
            self.advance()

            if self.match(TokenType.KEYWORD, "IN"):
                self.expect(TokenType.OP, "(")
                values = self.parse_expr_list()
                self.expect(TokenType.OP, ")")
                return InList(left, values, negated=True)
            
            if self.match(TokenType.KEYWORD, "LIKE"):
                return BinaryOp("NOT LIKE", left, self.parse_add())

            if self.match(TokenType.KEYWORD, "BETWEEN"):
                low = self.parse_add()
                self.expect(TokenType.KEYWORD, "AND")   
                high = self.parse_add()
                return Between(left, low, high, negated=True)
            

        
        if t.ttype == TokenType.KEYWORD and t.value == "IN":
            self.advance()
            self.expect(TokenType.OP, "(")
            values = self.parse_expr_list()

            self.expect(TokenType.OP, ")")
            return InList(left, valiues, negated=False)
        
        return left

    
    def parse_add(self) -> Any:
        left = self.parse_mul()
        while self.check(TokenType.OP, "+") or self.check(TokenType.OP, "-"):
            op = self.advance().value
            left = BinaryOp(op, left, self.parse_mul())
        return left
    
    def parse_mul(self) -> Any:
        left = self.parse_unary()
        while self.check(TokenType.OP, "*") or self.check(TokenType.OP, "/"):
            op = self.advance().value
            left = BinaryOp(op, left, self.parse_unary())
        return left
    
    def parse_unary(self) -> Any:
        if self.check(TokenType.OP, "-"):
            self.advance()
            return UnaryOp("-", self.parse_primary())
        return self.parse_primary()
    
    def parse_primary(self) -> Any:
        t = self.peek()

        if t.ttype == TokenType.OP and t.value == "(":
            self.advance()
            if self.check(TokenType.KEYWORD, "SELECT"):
                sub = self.parse_select()
                self.expect(TokenType.OP, ")")
                return Subquery(sub)
            
            expr = self.parse_expr()
            self.expect(TokenType.OP, ")")
            return expr
        

        if t.ttype == TokenType.KEYWORD and t.value == "CASE":
            return self.parse_case()
        
        if t.type == TokenType.NUMBER:
            self.advance()
            v = t.value

            return Literal(float(v) if "." in v else int(v))
        
        if t.ttype == TokenType.STRING:
            self.advance()
            return Literal(t.value)
        
        if t.ttype == TokenType.KEYWORD and t.value == "NULL":
            self.advance()
            return Literal(None)
        
        # function call

        if t.ttype == TokenType.IDENTIFIER or (
            t.ttype == TokenType.KEYWORD and t.value in ("COUNT", " SUM", "AVG","MIN", "MAX")
        ):
            name = self.advance().value

            if self.check(TokenType.OP, "("):
                self.advance()

                distinct = bool(self.match(TokenType.KEYWORD, "DISTINC"))

                if self.check(TokenType.OP, "*"):
                    self.advance()
                    args = [Star()]
                 
                elif self.check(TokenType.OP, ")"):
                    args = []
                
                else:
                    args = self.parse_expr_list()
                
                self.expect(TokenType.OP, ")")
                return FunctionCall(name.upper(), args, distinct)
            
            return Identifier(name)
        
        if t.ttype == TokenType.OP and t.value == "*":
            self.advance()
            return Star()
        
        raise ParseError(f"Unexpected token in expression: {t.value!r} at pos {t.pos}")

    
    def parse_case(self) -> Any:
        self.expect(TokenType.KEYWORD, "CASE")
        operand = None
        if not self.check(TokenType.KEYWORD, "WHEN"):
            operand = self.parse_expr()
        
        whens = []
        while self.match(TokenType.KEYWORD, "WHEN"):
            cond = self.parse_expr()
            self.expect(TokenType.KEYWORD, "THEN")
            result = self.parse_expr()
            whens.append((cond, result))
        
        else_ = None

        if self.match(TokenType.KEYWORD, "ELSE"):
            else_ = self.parse_expr()
        
        self.expect(TokenType.KEYWORD, "END")
        return CaseExpr(operand, whens, else_)
    


    
        
            