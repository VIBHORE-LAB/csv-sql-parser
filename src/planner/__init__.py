from dataclasses import dataclass, field
from typing import Any, List, Optional

from src.parser.ast import *

@dataclass
class StepLoadCSV:
    table_name: str
    alias: Optional[str]
    file_path: str


@dataclass
class StepFilter:
    condition: Any
    target: str


@dataclass
class StepJoin:
    join_type: str
    left: str
    right: str
    condition: Any



@dataclass
class StepAggregate:
    group_by: List[Any]
    aggregate: List[Any]
    having: Optional[Any]

@dataclass
class StepProject:
    columns: List[Any]
    distinct: bool

@dataclass
class StepSort:
    order_by: List[tuple]

@dataclass
class StepLimit:
    limit: Optional[int]
    offset: Optional[int]

@dataclass
class StepCreateTable:
    table_name: str
    columns: List[ColumnDef]
    if_not_exists: bool

@dataclass
class StepAlterTable:
    table_name: str
    action: str
    column: Optional[ColumnDef]
    column_name: Optional[str]

@dataclass
class StepInsert:
    table_name: str
    columns: Optional[List[str]]
    values: List[List[Any]]

@dataclass
class StepUpdate:
    table_name: str
    assignments: List[tuple]
    where: Optional[Any]

@dataclass
class StepDelete:
    table_name: str
    where: Optional[Any]


@dataclass
class ExecutionPlan:
    steps: List[Any] = field(default_factory=list)
    def add(self,step):
        self.steps.append(step)
        return self
    
    def __repr__(self):
        lines = ["ExecutionPlan:"]
        for i,step in enumerate(self.steps):
            lines.append(f"  {i+1}. {s.__class__.__name__}: {s}")
        return "\n".join(lines)


class Planner:  
    def plan(self, ast: Any) -> ExecutionPlan:
        if isinstance(ast, SelectStatement):
            return self._plan_select(ast)
        
        elif isinstance(ast, CreateTable):
            return ExecutionPlan([
                StepCreateTable(ast.name, ast.columns, ast.if_not_exists)
            ])
        
        elif isinstance(ast, AlterTable):
            return ExecutionPlan([
                StepAlterTable(ast.name, ast.action, ast.column, ast.column_name)
            ])
        elif isinstance(ast, InsertInto):
            return ExecutionPlan([
                StepInsert(ast.table, ast.columns, ast.values)
            ])
        elif isinstance(ast, UpdateStatement):
            return ExecutionPlan([
                StepUpdate(ast.table, ast.assignments, ast.where)
            ])
        
        elif isinstance(ast, DeleteStatement):
            return ExecutionPlan([
                StepDelete(ast.table, ast.where)
            ])
        raise ValueError(f"Unknown AST node type: {type(ast)}")

    def _plan_select(self, stmt: SelectStatement) -> ExecutionPlan:
        plan = ExecutionPlan()

        if stmt.from_table:
            plan.add(StepLoadCSV(
                table_name=stmt.from_table.name,
                alias=stmt.from_table.alias,
                file_path=stmt.from_table.name,
            ))
        
        for join in stmt.joins:
            plan.add(StepLoadCSV(
                table_name=join.table.name,
                alias=join.table.alias,
                file_path=join.table.name,
            ))

            plan.add(StepJoin(
                join_type=join.join_type,
                left = (stmt.from_table.alias or stmt.from_table.name if stmt.from_table else "__result__"),
                right=join.table.alias or join.table.name,
                condition=join.condition
            ))
        
        if stmt.where:
            plan.add(StepFilter(
                condition=stmt.where,
                target="__result__",
            ))
        has_agg = (
            self._has_aggregates(stmt.columns)
            or stmt.group_by
            or stmt.having
        )

        if has_agg:
            plan.add(StepAggregate(
                group_by=stmt.group_by,
                aggregates=self._extract_aggregates(stmt.columns),
                having=stmt.having,
                ))
        
        plan.add(StepProject(
            columns=stmt.columns,
            distinct=stmt.distinct,
        ))

        if stmt.order_by:
            plan.add(StepSort(order_by=stmt.order_by))
        
        if stmt.limit is not None or stmt.offset is not None:
            plan.add(StepLimit(limit=stmt.limit, offset=stmt.offset))
        
        return plan
    

    def _has_aggregates(self,column) -> bool:
        for col in columns:
            if self._contains_aggregate(col):
                return True
        return False
    
    def _contains_aggregate(self, node) -> bool:
        if isinstance(node, FunctionCall) and node.name.upper() in ["COUNT","SUM","AVG","MIN","MAX"]:
            return True
        if isinstance(node, Alias):
            return self._contains_aggregate(node.expr)
        if isinstance(node, BinaryOp):
            return (self._contains_aggregate(node.left) or
                    self._contains_aggregate(node.right))
 
        return False   # Literal, Identifier, Star — no aggregate
 
    def _extract_aggregates(self, columns) -> List[Any]:
        """
        Collect all aggregate FunctionCall nodes from the SELECT column list.
        The Executor's aggregation step needs these to know which aggregates
        to compute for each group.
        """
        aggs = []
        for col in columns:
            self._collect_aggs(col, aggs)    
        return aggs
 
    def _collect_aggs(self, node, out: list):
   
        if isinstance(node, FunctionCall) and node.name in ("COUNT","SUM","AVG","MIN","MAX"):
            out.append(node)   
 
        elif isinstance(node, Alias):
            self._collect_aggs(node.expr, out)   
 
        elif isinstance(node, BinaryOp):
            self._collect_aggs(node.left, out)   
            self._collect_aggs(node.right, out)  
 
 
 
 
def plan(ast: Any) -> ExecutionPlan:
 
    return Planner().plan(ast)
 
