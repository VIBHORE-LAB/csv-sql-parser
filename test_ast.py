# test_ast.py
import sys; sys.path.insert(0, '.')
from src.parser.ast import (
    Literal, Identifier, BinaryOp, SelectStatement,
    TableRef, Alias, FunctionCall, Star, CreateTable, ColumnDef
)

# Test 1: nodes are printable (repr works)
lit = Literal(42)
assert lit.value == 42
print(f"✓ Literal: {lit}")

# Test 2: nested tree builds correctly
expr = BinaryOp('>', Identifier('age'), Literal(18))
assert expr.op == '>'
assert isinstance(expr.left, Identifier)
assert expr.left.name == 'age'
assert isinstance(expr.right, Literal)
assert expr.right.value == 18
print(f"✓ BinaryOp tree: {expr}")

# Test 3: SelectStatement defaults are correct
stmt = SelectStatement(columns=[Star()])
assert stmt.from_table is None
assert stmt.where is None
assert stmt.group_by == []
assert stmt.order_by == []
assert stmt.distinct == False
print(f"✓ SelectStatement defaults: {stmt}")

# Test 4: Alias wraps correctly
alias = Alias(FunctionCall('COUNT', [Star()]), 'headcount')
assert alias.name == 'headcount'
print(f"✓ Alias + FunctionCall: {alias}")

print("\nAll AST tests passed.")