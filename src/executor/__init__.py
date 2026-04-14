import csv
import os
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from src.planner import *
from src.parser.ast import *
from src.executor.evaluator import eval_expr, EvalError, coerce

class ExecutionError(Exception):
    pass

class Executor:
    def __init__(self,csv_dir: str = "."):
        self.csv_dir = csv_dir
        self.relations: Dict[str, List[dict]] = {}
        self.result: List[dict] = []
        self.result_columns: List[str] = []
        self.message: Optional[str] = None

    def run(self, plan: ExecutionPlan) -> Tuple[List[str],List[dict]]:
        self.relations = {}
        self.result = []
        self.result_columns = []
        self.message = None

        for step in plan.steps:
            self._run_step(step)
        return self.result_columns, self.result

    def _run_step(self,step):
        if isinstance(step, StepLoadCSV):
            self._load_csv(step)
        elif isinstance(step, StepFilter):
            self._filter(step)
        elif isinstance(step, StepJoin):
            self._join(step)
        elif isinstance(step, StepAggregate):
            self._aggregate(step)
        elif isinstance(step, StepProject):
            self._project(step)
        elif isinstance(step, StepSort):
            self._sort(step)
        elif isinstance(step, StepLimit):
            self._limit(step)
        elif isinstance(step, StepCreateTable):
            self._create_table(step)
        elif isinstance(step, StepAlterTable):
            self._alter_table(step)
        elif isinstance(step, StepInsert):
            self._insert(step)
        elif isinstance(step, StepUpdate):
            self._update(step)
        elif isinstance(step, StepDelete):
            self._delete(step)
        else:
            raise ExecutionError(f"Unknown plan step: {type(step)}")

    def _csv_path(self,name: str) -> str:
        path = os.path.join(self.csv_dir, f"{name}")
        if not os.path.exists(path):
            exact = os.path.join(self.csv_dir, name)

            if os.path.exists(exact):
                return exact
        return path
    def _load_csv(self, step: StepLoadCSV):
        path = self._csv_path(step.file_path)
        if not os.path.exists(path):
            raise ExecutionError(f"File not found: {path}")
        rows = []
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rel_name = step.alias or step.table_name

            for raw in reader:
                row = {}
                for col, val in raw.items():
                    col = col.strip()
                    val = val.strip() if val else val

                    row[f"{rel_name}.{col}"] = coerce(val) if val!="" else None
                    row[col] = coerce(val) if val!="" else None
                rows.append(row)
        rel_name = step.alias or step.table_name
        self.relations[rel_name] = rows
        if not self.result:
            self.result = rows
    def _save_csv(self, table_name: str, rows: List[dict], columns: List[str]):
        path = self._csv_path(table_name)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    c: row.get(f"{table_name}.{c}", row.get(c, ""))
                    for c in columns
                })
    def _filter(self,step: StepFilter):
        filtered = []
        for row in self.result:
            try:
                if eval_expr(step.condition, row):
                    filtered.append(row)
            except EvalError:
                pass

        self.result = filtered

    def _join(self,step: StepJoin):
        left_rows = self.result
        right_rows = self.relations.get(step.right,[])
        joined = []
        if step.join_type == "INNER":
            for l_row in left_rows:
                matched = False
                for r_row in right_rows:
                    merged = {**l_row, **r_row}
                    try:
                        if eval_expr(step.condition, merged):
                            joined.append(merged)
                            matched = True
                    except EvalError:
                        pass
                if not matched:
                    null_r = {k: None for k in (right_rows[0].keys() if right_rows else [])}
                    joined.append({**l_row, **null_r})
        elif step.join_type == "RIGHT":
            for r_row in right_rows:
                matched = False
                for l_row in left_rows:
                    merged={**l_row, **r_row}
                    try:
                        if eval_expr(step.condition,merged):
                            joined.append(merged)
                            matched = True
                    except EvalError:
                        pass
                if not matched:
                    null_l = {k: None for k in (left_rows[0].keys() if left_rows else [])}
                    joined.append({**null_l, **r_row})
        elif step.join_type == "LEFT":
            for l_row in left_rows:
                matched = False

                for r_row in right_rows:
                    merged = {**l_row, **r_row}
                    try:
                        if eval_expr(step.condition, merged):
                            joined.append(merged)
                            matched = True
                    except EvalError:
                        pass
                if not matched:
                    null_r = {k: None for k in (right_rows[0].keys() if right_rows else [])}
                    joined.append({**l_row, **null_r})
        elif step.join_type == "FULL":
            left_matched = set()
            right_matched = set()

            for i, l_row in enumerate(left_rows):
                for j, r_row in enumerate(right_rows):
                    merged = {**l_row, **r_row}
                    try:
                        if eval_expr(step.condition, merged):
                            joined.append(merged)
                            left_matched.add(i)
                            right_matched.add(j)
                    except EvalError:
                        pass
            for i, l_row in enumerate(left_rows):
                if i not in left_matched:
                    null_r = {k: None for k in (right_rows[0].keys() if right_rows else [])}
                    joined.append({**l_row, **null_r})
            for j, r_row in enumerate(right_rows):
                if j not in right_matched:
                    null_l = {k: None for k in (left_rows[0].keys() if left_rows else [])}
                    joined.append({**null_l, **r_row})
        self.result = joined
    def _aggregate(self,step: StepAggregate):
        groups: Dict[tuple, List[dict]] = defaultdict(list)
        for row in self.result:
            if step.group_by:
                key = tuple(eval_expr(e,row) for e in step.group_by)
            else:
                key = ("__all__",)
            groups[key].append(row)
        result = []
        for key, rows in groups.items():
            agg_row = {}
            if step.group_by:
                for i,expr in enumerate(step.group_by):
                    col_name = self._expr_name(expr)
                    agg_row[col_name] = key[i]

            for agg in step.aggregates:
                agg_key = f"{agg.name}({self._expr_name(agg.args[0]) if agg.args else '*'})"
                if agg.name == "COUNT":
                    if isinstance(agg.args[0], Star):
                        agg_row[agg_key] = len(rows)
                    else:
                        vals = [eval_expr(agg.args[0],r) for r in rows
                                if eval_expr(agg.args[0],r) is not None]
                        if agg.distinct:
                            vals = list(set(vals))
                        agg_row[agg_key] = len(vals)
                elif agg.name == "SUM":
                    vals = [eval_expr(agg.args[0], r) for r in rows]
                    agg_row[agg_key] = sum(v for v in vals if v is not None)
                elif agg.name == "AVG":
                    vals = [eval_expr(agg.args[0], r) for r in rows
                            if eval_expr(agg.args[0], r) is not None]
                    agg_row[agg_key] = sum(vals) / len(vals) if vals else None
                elif agg.name == "MIN":
                    vals = [eval_expr(agg.args[0], r) for r in rows
                            if eval_expr(agg.args[0], r) is not None]
                    agg_row[agg_key] = min(vals) if vals else None
                elif agg.name == "MAX":
                    vals = [eval_expr(agg.args[0], r) for r in rows
                            if eval_expr(agg.args[0], r) is not None]
                    agg_row[agg_key] = max(vals) if vals else None
            agg_row.update(rows[0])
            result.append(agg_row)
        if step.having:
            result = [r for r in result if eval_expr(step.having, r)]
        self.result = result
    def _project(self, step: StepProject):
        AGG_FUNCS = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
        if any(isinstance(c, Star) and c.table is None for c in step.columns):
            if self.result:
                seen = set()
                cols = []
                for k in self.result[0].keys():
                    bare = k.split(".", 1)[1] if "." in k else k
                    if bare not in seen:
                        seen.add(bare)
                        cols.append(bare)
                self.result_columns = cols
            return
        projected = []
        out_cols  = []
        for row in self.result:
            new_row = {}
            for col in step.columns:
                if isinstance(col, Alias):
                    inner = col.expr
                    if isinstance(inner, FunctionCall) and inner.name in AGG_FUNCS:
                        agg_key = f"{inner.name}({self._expr_name(inner.args[0]) if inner.args else '*'})"
                        val = row.get(col.name, row.get(agg_key))
                    else:
                        val = eval_expr(inner, row)
                    new_row[col.name] = val
                    if col.name not in out_cols:
                        out_cols.append(col.name)
                elif isinstance(col, Identifier):
                    val  = eval_expr(col, row)
                    name = col.name
                    new_row[name] = val
                    if name not in out_cols:
                        out_cols.append(name)
                elif isinstance(col, FunctionCall) and col.name in AGG_FUNCS:
                    name = f"{col.name}({self._expr_name(col.args[0]) if col.args else '*'})"
                    val  = row.get(name)
                    new_row[name] = val
                    if name not in out_cols:
                        out_cols.append(name)
                elif isinstance(col, FunctionCall):
                    val  = eval_expr(col, row)
                    name = f"{col.name}({self._expr_name(col.args[0]) if col.args else '*'})"
                    new_row[name] = val
                    if name not in out_cols:
                        out_cols.append(name)
                else:
                    val  = eval_expr(col, row)
                    name = str(col)
                    new_row[name] = val
                    if name not in out_cols:
                        out_cols.append(name)
            projected.append(new_row)
        if step.distinct:
            seen = set()
            deduped = []
            for row in projected:
                key = tuple(row.values())
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            projected = deduped
        self.result = projected
        self.result_columns = out_cols
    def _sort(self, step: StepSort):
        result = self.result[:]
        for i in range(len(step.order_by) - 1, -1, -1):
            expr, direction = step.order_by[i]
            def sort_key(row, e=expr):
                val = eval_expr(e, row)
                return (val is None, val if val is not None else "")
            result.sort(
                key=sort_key,
                reverse=(direction == "DESC"),
            )
        self.result = result
    def _limit(self, step: StepLimit):
        offset = step.offset or 0
        data   = self.result[offset:]
        if step.limit is not None:
            data = data[:step.limit]
        self.result = data
    def _create_table(self, step: StepCreateTable):
        path = self._csv_path(step.table_name)
        if os.path.exists(path):
            if step.if_not_exists:
                msg = f"Table '{step.table_name}' already exists (skipped)"
                self.result_columns = ["message"]
                self.result = [{"message": msg}]
                return
            raise ExecutionError(f"Table '{step.table_name}' already exists")
        cols = [c.name for c in step.columns]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
        self.result_columns = ["message"]
        self.result = [{"message": f"Table '{step.table_name}' created with columns: {', '.join(cols)}"}]
    def _alter_table(self, step: StepAlterTable):
        path = self._csv_path(step.table_name)
        if not os.path.exists(path):
            raise ExecutionError(f"Table '{step.table_name}' not found")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_cols = reader.fieldnames or []
            rows = list(reader)
        if step.action == "ADD_COLUMN":
            col_name = step.column.name
            if col_name in existing_cols:
                raise ExecutionError(f"Column '{col_name}' already exists")
            new_cols = existing_cols + [col_name]
            default = step.column.default
            for row in rows:
                if default is not None and hasattr(default, 'value'):
                    row[col_name] = default.value
                else:
                    row[col_name] = default
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=new_cols)
                writer.writeheader()
                writer.writerows(rows)
            self.result_columns = ["message"]
            self.result = [{"message": f"Column '{col_name}' added to '{step.table_name}'"}]
        elif step.action == "DROP_COLUMN":
            col_name = step.column_name
            if col_name not in existing_cols:
                raise ExecutionError(
                    f"Column '{col_name}' not found in '{step.table_name}'"
                )
            new_cols = [c for c in existing_cols if c != col_name]
            for row in rows:
                row.pop(col_name, None)
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=new_cols)
                writer.writeheader()
                writer.writerows(rows)
            self.result_columns = ["message"]
            self.result = [{"message": f"Column '{col_name}' dropped from '{step.table_name}'"}]
    def _insert(self, step: StepInsert):
        path = self._csv_path(step.table_name)
        if not os.path.exists(path):
            raise ExecutionError(f"Table '{step.table_name}' not found")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_cols = reader.fieldnames or []
            rows = list(reader)
        for value_row in step.values:
            if step.columns:
                new_row = {c: "" for c in existing_cols}
                for col, val in zip(step.columns, value_row):
                    v = val.value if isinstance(val, Literal) else str(val)
                    new_row[col] = "" if v is None else v
            else:
                if len(value_row) != len(existing_cols):
                    raise ExecutionError(
                        f"Column count mismatch: table has {len(existing_cols)} columns, "
                        f"INSERT provides {len(value_row)} values"
                    )
                new_row = {}
                for col, val in zip(existing_cols, value_row):
                    v = val.value if isinstance(val, Literal) else str(val)
                    new_row[col] = "" if v is None else v
            rows.append(new_row)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=existing_cols)
            writer.writeheader()
            writer.writerows(rows)
        self.result_columns = ["message"]
        self.result = [{"message": f"{len(step.values)} row(s) inserted into '{step.table_name}'"}]
    def _update(self, step: StepUpdate):
        path = self._csv_path(step.table_name)
        if not os.path.exists(path):
            raise ExecutionError(f"Table '{step.table_name}' not found")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_cols = reader.fieldnames or []
            rows = list(reader)
        updated = 0
        for row in rows:
            should_update = (
                step.where is None
                or eval_expr(step.where, row)
            )
            if should_update:
                for col, expr in step.assignments:
                    row[col] = eval_expr(expr, row)
                updated += 1
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=existing_cols)
            writer.writeheader()
            writer.writerows(rows)
        self.result_columns = ["message"]
        self.result = [{"message": f"{updated} row(s) updated in '{step.table_name}'"}]
    def _delete(self, step: StepDelete):
        path = self._csv_path(step.table_name)
        if not os.path.exists(path):
            raise ExecutionError(f"Table '{step.table_name}' not found")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_cols = reader.fieldnames or []
            rows = list(reader)
        kept    = []
        deleted = 0
        for row in rows:
            should_delete = (
                step.where is None
                or eval_expr(step.where, row)
            )
            if should_delete:
                deleted += 1
            else:
                kept.append(row)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=existing_cols)
            writer.writeheader()
            writer.writerows(kept)
        self.result_columns = ["message"]
        self.result = [{"message": f"{deleted} row(s) deleted from '{step.table_name}'"}]
    def _expr_name(self, node) -> str:
        if isinstance(node, Identifier):
            return node.name
        if isinstance(node, Alias):
            return node.name
        if isinstance(node, Star):
            return "*"
        if isinstance(node, FunctionCall):
            arg_name = self._expr_name(node.args[0]) if node.args else '*'
            return f"{node.name}({arg_name})"
        if isinstance(node, Literal):
            return str(node.value)
        return str(node)
