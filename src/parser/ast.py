## abstract syntax tree

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Any


@dataclass
class Literal:
    value: Any

@dataclass
class Star:
    table: Optional[str]

@dataclass
class BinaryOp:
    op: str
    left: Any
    right: Any

@dataclass
class UnaryOp:
    op: str
    operand: Any

@dataclass
class FunctionCall:
    name: str
    args: List[Any]
    distinct: bool = False

@dataclass
class Alias:
    expr: Any
    name: str

@dataclass
class InList:
    expr: Any
    values: List[Any]
    negated: bool = False

@dataclass
class IsNull:
    expr: Any
    negated: bool = False

@dataclass
class Between:
    expr: Any
    low: Any
    high: Any
    negated: bool = False

@dataclass
class CaseExpr:
    operand: Optional[Any]
    whens: List[tuple]
    else_: Optional[Any]


@dataclass
class SubQuery:
    select: Any
    alias: Optional[str] = None

@dataclass
class TableRef:
    name: str
    alias: Optional[str] = None

@dataclass
class JoinClause:
    join_type: str
    table: TableRef
    condition: Any

@dataclass
class SelectStatement:
    columns: List[Any]
    from_table: Optional[TableRef] = None
    joins: List[JoinClause] = field(default_factory=list)
    where: Optional[Any] = None
    group_by: List[Any] = field(default_factory=list)
    having: Optional[Any] = None
    order_by: List[Any] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    distinct: bool = False

@dataclass
class ColumnDef:
    name: str
    type_: str
    default: Optional[Any] = None
    primary_key: bool = False

@dataclass
class CreateTable:
    name: str
    columns: List[ColumnDef]
    if_not_exists: bool = False

@dataclass
class AlterTable:
    name: str
    action: str
    column: Optional[ColumnDef] = None
    column_name: Optional[str] = None

@dataclass
class InsertInto:
    table: str
    columns: Optional[List[str]]
    values: List[List[Any]]

@dataclass
class UpdateStatement:
    table: str                
    assignments: List[tuple]   
    where: Optional[Any] = None 
 
 
@dataclass
class DeleteStatement:
   
    table: str             
    where: Optional[Any] = None   

