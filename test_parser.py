# test_parser.py
import sys; sys.path.insert(0, '.')
from src.parser import parse
from src.parser.ast import (
    SelectStatement, BinaryOp, Identifier, Literal,
    FunctionCall, Alias, Star, CreateTable, InsertInto,
    JoinClause, AlterTable, DeleteStatement
)

# Test 1: basic SELECT
ast = parse("SELECT * FROM employees")
assert isinstance(ast, SelectStatement)
assert ast.from_table.name == "employees"
assert isinstance(ast.columns[0], Star)
print("✓ basic SELECT *")

# Test 2: WHERE with comparison
ast = parse("SELECT name FROM users WHERE age > 18")
assert isinstance(ast.where, BinaryOp)
assert ast.where.op == ">"
assert isinstance(ast.where.left, Identifier)
assert ast.where.left.name == "age"
print("✓ WHERE with BinaryOp")

# Test 3: GROUP BY + aggregate + alias
ast = parse("SELECT dept, COUNT(*) AS cnt FROM staff GROUP BY dept")
assert ast.group_by[0].name == "dept"
assert isinstance(ast.columns[1], Alias)
assert isinstance(ast.columns[1].expr, FunctionCall)
assert ast.columns[1].expr.name == "COUNT"
print("✓ GROUP BY + COUNT alias")

# Test 4: JOIN
ast = parse("SELECT * FROM a JOIN b ON a.id = b.id")
assert len(ast.joins) == 1
assert ast.joins[0].join_type == "INNER"
assert isinstance(ast.joins[0].condition, BinaryOp)
print("✓ INNER JOIN with ON condition")

# Test 5: ORDER BY direction
ast = parse("SELECT name FROM t ORDER BY salary DESC, name ASC")
assert ast.order_by[0][1] == "DESC"
assert ast.order_by[1][1] == "ASC"
print("✓ ORDER BY with directions")

# Test 6: CREATE TABLE
ast = parse("CREATE TABLE projects (id INTEGER, name VARCHAR)")
assert isinstance(ast, CreateTable)
assert ast.name == "projects"
assert ast.columns[0].name == "id"
print("✓ CREATE TABLE")

# Test 7: INSERT INTO
ast = parse("INSERT INTO t (id, name) VALUES (1, 'Alice')")
assert isinstance(ast, InsertInto)
assert ast.columns == ["id", "name"]
assert len(ast.values[0]) == 2
print("✓ INSERT INTO with named columns")

# Test 8: LIMIT + OFFSET
ast = parse("SELECT * FROM t LIMIT 10 OFFSET 20")
assert ast.limit == 10
assert ast.offset == 20
print("✓ LIMIT + OFFSET")

print("\nAll parser tests passed.")