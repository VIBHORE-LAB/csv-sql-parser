# test_lexer.py
import sys
sys.path.insert(0, '.')
from src.lexer import tokenize, TokenType, LexError

# Sample SQL queries for stress-testing the lexer
SAMPLE_QUERIES = [
    # 1. Basic Select
    "SELECT name, age FROM users WHERE age > 18",

    # 2. Joins and Aliases
    """
    SELECT u.name, o.total
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE o.status = 'COMPLETED'
    ORDER BY o.total DESC
    """,

    # 3. Aggregates and GROUP BY
    "SELECT category, COUNT(*), SUM(price) FROM products GROUP BY category HAVING COUNT(*) > 5",

    # 4. Insert Statement
    "INSERT INTO profiles (user_id, bio, rating) VALUES (101, 'Python developer & SQL enthusiast', 4.5)",

    # 5. Update Statement
    "UPDATE inventory SET stock = stock - 1 WHERE item_id = 'IT-999'",

    # 6. DDL Clause
    "CREATE TABLE IF NOT EXISTS logs (id PRIMARY KEY, msg DEFAULT 'Empty')",

    # 7. Complex strings and operators
    "SELECT * FROM data WHERE col1 != '' AND col2 <> col3 OR col4 <= 10.5",
    
    # 8. Formatting Stress
    "  SELECT\t*\nFROM\n  foo\r\nWHERE\n1=1  "
]

def print_tokens(query: str, tokens: list):
    print("-" * 60)
    print(f"QUERY: {query.strip()}")
    print("-" * 60)
    print(f"{'TYPE':<15} | {'VALUE':<25} | {'POS':<5}")
    print("-" * 60)
    for token in tokens:
        print(f"{token.ttype.name:<15} | {token.value!r:<25} | {token.pos:<5}")
    print("\n")

def run_tests():
    print("Running Stress Test Suite...\n")
    success_count = 0
    total_count = len(SAMPLE_QUERIES)

    for i, query in enumerate(SAMPLE_QUERIES, 1):
        try:
            tokens = tokenize(query)
            print_tokens(query, tokens)
            success_count += 1
        except LexError as e:
            print(f"FAILED Query {i}: {query.strip()}")
            print(f"ERROR: {e}\n")
        except Exception as e:
            print(f"UNEXPECTED ERROR Query {i}: {type(e).__name__}: {e}\n")

    print(f"Lexer Test Summary: {success_count}/{total_count} queries tokenized successfully.")

if __name__ == "__main__":
    run_tests()