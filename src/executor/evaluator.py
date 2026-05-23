import re
import fnmatch
from typing import Any,Dict, Optional

from src.parser.ast import *

class EvalError(Exception):
    pass

def coerce(v):
    if isinstance(v,str):
        try:
            return int(v)
        except ValueError:
            pass
    
        try:
            return float(v)
        except ValueError:
            pass
    
    return v

def eval_expr(node: Any, row:Dict[str,Any]) -> Any:
    if isinstance(node, Literal):
        return node.value
    
    if isinstance(node, Identifier):
        if node.table:
            key = f"{node.table}.{node.name}"
            
            if key in row:
                return coerce(row[key])
        
        for k,v in row.items():
            if k == node.name or k.endswith(f".{node.name}"):
                return coerce(v)
        
        qualified = f"{node.table}.{node.name}" if node.table else node.name
        raise EvalError(f"Column not found: {qualified}")

    if isinstance(node, Alias):
        return eval_expr(node.expr, row)
    
    if isinstance(node, UnaryOp):
        val = eval_expr(node.operand, row)
        
        if node.op == "-":
            return -val
        if node.op == "NOT":
            return not val
        raise EvalError(f"Unknown unary operator: {node.op}")
    
    if isinstance(node, BinaryOp):
        return eval_binary(node, row)

    if isinstance(node, IsNull):
        val = eval_expr(node.expr, row)
        result = (val is None)
        return not result if node.negated else result

    if isinstance(node, InList):
        val = eval_expr(node.expr, row)
        values = [eval_expr(v,row) for v in node.values]
        result = val in values
        return not result if node.negated else result

    if isinstance(node, Between):
        val = eval_expr(node.expr, row)
        low = eval_expr(node.low, row)
        high = eval_expr(node.high, row)
        result = (low <= val <= high)    
        return not result if node.negated else result
    
    if isinstance(node, CaseExpr):
        if node.operand is not None:
            base = eval_expr(node.operand, row)
            for cond, result in node.whens:
                if eval_expr(cond, row) == base:
                    return eval_expr(result, row)
            
        else:
            for cond, result in node.whens:
                if eval_expr(cond,row):
                    return eval_expr(result, row)
            
        return eval_expr(node.else_,row) if node.else_ is not None else None

    if isinstance(node, FunctionCall):
        if node.name == "UPPER":
            return str(eval_expr(node.args[0],row)).upper()
        if node.name == "LOWER":
            return str(eval_expr(node.args[0],row)).lower()
        if node.name == "LENGTH":
            return len(str(eval_expr(node.args[0],row)))
        if node.name == "SUBSTR":
            s = str(eval_expr(node.args[0],row))
            start = eval_expr(node.args[1],row)
            length = eval_expr(node.args[2],row) if len(node.args) > 2 else None
            return s[start-1:start-1+length] if length is not None else s[start-1:]
        if node.name == "CONCAT":
            return "".join(str(eval_expr(a,row)) for a in node.args)
        if node.name == "COUNT":
            return 1 if node.distinct else 1
        if node.name in ("SUM","AVG","MIN","MAX"):
            return 0 if node.distinct else 0
        if node.name == "COALESCE":
            for arg in node.args:
                v = eval_expr(arg, row)
                if v is not None:
                    return v     
            return None        
        raise EvalError(f"Unknown function: {node.name}")

    if isinstance(node, Star):
        return "*"
    
    raise EvalError(f"Unknown AST node type: {type(node).__name__}")

def eval_binary(node: BinaryOp, row: Dict) -> Any:
    op = node.op
    if op == "AND":
        left_val = bool(eval_expr(node.left, row))
        if not left_val:
            return False
        return bool(eval_expr(node.right, row))

    if op == "OR":
        left_val = bool(eval_expr(node.left, row))
        if left_val:
            return True

    left = eval_expr(node.left, row)
    right = eval_expr(node.right, row)

    if op == "+":
        if isinstance(left, str) or isinstance(right,str):
            return str(left) + str(right)
        return left + right
    
    if op == "-":
        return left - right
    if op == "*":
        return left * right
    if op == "/":
        if right == 0:
            raise EvalError("Division by zero")
        return left / right
    
    left = coerce(left)
    right = coerce(right)

    if op == "=":   return left == right   
    if op == "!=":  return left != right   
    if op == "<":   return left < right    
    if op == "<=":  return left <= right   
    if op == ">":   return left > right    
    if op == ">=":  return left >= right   
    
    if op == "LIKE":
        pattern = str(right).replace("%", "*").replace("_", "?")   
        return fnmatch.fnmatch(str(left).lower(), pattern.lower())  
 
    if op == "NOT LIKE":
        pattern = str(right).replace("%", "*").replace("_", "?")
        return not fnmatch.fnmatch(str(left).lower(), pattern.lower())
 
    raise EvalError(f"Unknown binary operator: {op}")
