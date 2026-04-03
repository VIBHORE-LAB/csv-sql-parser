import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import List


class TokenType(Enum):
    KEYWORD = auto()
    NUMBER = auto()
    STRING = auto()
    OP = auto()
    IDENTIFIER = auto()
    WS = auto()
    EOF = auto()


KEYWORDS = {
    # Query clauses
    "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL",
    "OUTER", "ON", "GROUP", "BY", "ORDER", "ASC", "DESC", "HAVING",
    "LIMIT", "OFFSET", "AS", "DISTINCT", "UNION", "ALL",

    # Boolean / comparison operators (word form)
    "AND", "OR", "NOT", "IN", "LIKE", "IS", "NULL", "BETWEEN", "EXISTS",

    # Aggregate functions  (treated as keywords so they tokenise correctly
    #                       even when written in uppercase in SQL)
    "COUNT", "SUM", "AVG", "MIN", "MAX",

    # DDL — Data Definition Language
    "CREATE", "TABLE", "ALTER", "ADD", "DROP", "COLUMN",
    "PRIMARY", "KEY", "DEFAULT", "IF",

    # DML — Data Manipulation Language
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",

    # Control flow
    "CASE", "WHEN", "THEN", "ELSE", "END",
}


_TOKEN_RE = re.compile(
    # STRING
    r"(?P<STRING>'(?:[^'\\]|\\.)*')"

    # NUMBER
    r"|(?P<NUMBER>\b\d+(?:\.\d+)?\b)"

    # OPERATORS
    r"|(?P<OP>!=|<=|>=|<>|[=<>()\[\],.*;\+\-/])"

    # IDENTIFIER
    r"|(?P<IDENT_OR_KW>[A-Za-z_][A-Za-z0-9_]*)"

    # WHITESPACE
    r"|(?P<WS>\s+)"

    # MISMATCH
    r"|(?P<MISMATCH>.)",

    re.IGNORECASE
)


@dataclass
class Token:
    ttype: TokenType
    value: str
    pos: int

    def __repr__(self):
        return f"Token({self.ttype.name}, {self.value!r})"


class LexError(Exception):
    pass


def tokenize(sql: str) -> List[Token]:
    tokens: List[Token] = []

    for m in _TOKEN_RE.finditer(sql):
        kind = m.lastgroup
        value = m.group()
        pos = m.start()

        if kind == "WS":
            continue

        elif kind == "MISMATCH":
            raise LexError(f"Unexpected character {value!r} at position {pos}")

        elif kind == "STRING":
            tokens.append(Token(TokenType.STRING, value[1:-1], pos))

        elif kind == "NUMBER":
            tokens.append(Token(TokenType.NUMBER, value, pos))

        elif kind == "OP":
            tokens.append(Token(TokenType.OP, value, pos))

        elif kind == "IDENT_OR_KW":
            if value.upper() in KEYWORDS:
                tokens.append(Token(TokenType.KEYWORD, value.upper(), pos))

            else:
                tokens.append(Token(TokenType.IDENTIFIER, value, pos))

    tokens.append(Token(TokenType.EOF, "", len(sql)))

    return tokens