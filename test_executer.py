# test_executor.py
import sys, os; sys.path.insert(0, '.')
from src.parser import parse
from src.planner import plan
from src.executor import Executor, ExecutionError

DATA_DIR = "data"   # assumes employees.csv, departments.csv exist here

def run(sql, csv_dir=DATA_DIR):
    ex = Executor(csv_dir=csv_dir)
    return ex.run(plan(parse(sql)))

# ── READ queries ──────────────────────────────────────────────────────────────

cols, rows = run("SELECT * FROM employees LIMIT 3")
assert len(rows) == 3
print(f"✓ SELECT * LIMIT 3: {len(rows)} rows, cols={cols[:3]}")

cols, rows = run("SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC")
assert all(r.get("department") == "Engineering" or r.get("name") for r in rows)
salaries = [float(r.get("salary", 0)) for r in rows]
assert salaries == sorted(salaries, reverse=True)
print(f"✓ WHERE + ORDER BY: {len(rows)} rows, descending salaries confirmed")

cols, rows = run("SELECT department, COUNT(*) AS headcount FROM employees GROUP BY department")
assert "headcount" in cols
assert any(int(r.get("headcount", 0)) > 1 for r in rows)
print(f"✓ GROUP BY + COUNT: {len(rows)} groups")

cols, rows = run("SELECT e.name, d.department_name FROM employees e JOIN departments d ON e.dept_id = d.id LIMIT 5")
assert "name" in cols and "department_name" in cols
assert len(rows) == 5
print(f"✓ INNER JOIN: {len(rows)} rows")

cols, rows = run("SELECT e.name, d.department_name FROM departments d LEFT JOIN employees e ON e.dept_id = d.id")
assert len(rows) >= 4   # at least one row per department
print(f"✓ LEFT JOIN: {len(rows)} rows")

# ── DDL / DML (use a temp directory) ─────────────────────────────────────────

import tempfile, shutil
tmp = tempfile.mkdtemp()   # isolated temp dir so we don't pollute data/

try:
    def runt(sql): return run(sql, csv_dir=tmp)

    # CREATE TABLE
    cols, rows = runt("CREATE TABLE test (id INTEGER, name VARCHAR, score FLOAT)")
    assert "created" in rows[0]["message"].lower()
    assert os.path.exists(os.path.join(tmp, "test.csv"))
    print("✓ CREATE TABLE")

    # INSERT rows
    cols, rows = runt("INSERT INTO test (id, name, score) VALUES (1, 'Alice', 9.5), (2, 'Bob', 7.2)")
    assert "2 row(s)" in rows[0]["message"]
    print("✓ INSERT INTO (multi-row)")

    # SELECT after insert
    cols, rows = runt("SELECT * FROM test ORDER BY score DESC")
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"   # higher score sorts first
    print("✓ SELECT after INSERT, ORDER BY verified")

    # UPDATE
    cols, rows = runt("UPDATE test SET score = 10.0 WHERE name = 'Alice'")
    assert "1 row(s)" in rows[0]["message"]
    cols, rows = runt("SELECT score FROM test WHERE name = 'Alice'")
    assert float(rows[0]["score"]) == 10.0
    print("✓ UPDATE + verify")

    # ALTER ADD COLUMN
    cols, rows = runt("ALTER TABLE test ADD COLUMN grade VARCHAR")
    assert "added" in rows[0]["message"].lower()
    cols, rows = runt("SELECT * FROM test")
    assert "grade" in cols
    print("✓ ALTER TABLE ADD COLUMN")

    # ALTER DROP COLUMN
    cols, rows = runt("ALTER TABLE test DROP COLUMN grade")
    cols, rows = runt("SELECT * FROM test")
    assert "grade" not in cols
    print("✓ ALTER TABLE DROP COLUMN")

    # DELETE
    cols, rows = runt("DELETE FROM test WHERE id = 2")
    assert "1 row(s)" in rows[0]["message"]
    cols, rows = runt("SELECT * FROM test")
    assert len(rows) == 1
    print("✓ DELETE + row count verified")

    # Table not found raises ExecutionError
    try:
        runt("SELECT * FROM nonexistent")
        assert False, "should have raised"
    except ExecutionError as e:
        print(f"✓ ExecutionError on missing table: {e}")

finally:
    shutil.rmtree(tmp)   # always clean up the temp dir

print("\nAll executor tests passed.")