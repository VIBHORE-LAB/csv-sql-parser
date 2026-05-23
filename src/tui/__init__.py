import os
import time
import csv
import re
from pathlib import Path
from typing import List, Tuple, Optional, Dict

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, ScrollableContainer
from textual.widgets import (
    Header,
    Footer,
    Static,
    DataTable,
    TextArea,
    Label,
    ListView,
    ListItem,
    Button,
    Tree,
)
from textual.screen import ModalScreen
from textual.reactive import reactive
from textual import events
from rich.text import Text

from src.lexer import LexError
from src.parser import parse, ParseError
from src.planner import plan
from src.executor import Executor, ExecutionError

VERSION = "1.0.0"

# ── SQL keyword completions ──────────────────────────────────────────────────
SQL_KEYWORDS = [
    "SELECT", "FROM", "WHERE", "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN",
    "FULL JOIN", "ON", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS NULL",
    "IS NOT NULL", "AS", "DISTINCT", "ORDER BY", "GROUP BY", "HAVING", "LIMIT",
    "OFFSET", "COUNT", "SUM", "AVG", "MIN", "MAX", "INSERT INTO", "VALUES",
    "UPDATE", "SET", "DELETE FROM", "CREATE TABLE", "ALTER TABLE", "DROP TABLE",
    "ADD COLUMN", "DROP COLUMN", "PRIMARY KEY", "INTEGER", "VARCHAR", "FLOAT",
    "BOOLEAN", "DATE", "ASC", "DESC", "UNION", "UNION ALL", "EXISTS", "CASE",
    "WHEN", "THEN", "ELSE", "END",
]

SAMPLE_QUERIES = [
    ("Basic SELECT",   "SELECT * FROM employees LIMIT 10"),
    ("Filtered query", "SELECT name, salary FROM employees\nWHERE department = 'Engineering'\nORDER BY salary DESC"),
    ("JOIN tables",    "SELECT e.name, d.department_name\nFROM employees e\nJOIN departments d ON e.dept_id = d.id"),
    ("GROUP BY + agg", "SELECT department, COUNT(*) AS headcount,\n       AVG(salary) AS avg_salary\nFROM employees\nGROUP BY department\nORDER BY avg_salary DESC"),
    ("Orders by customer", "SELECT c.name, o.amount, o.status\nFROM orders o\nJOIN customers c ON o.customer_id = c.id\nORDER BY o.amount DESC"),
    ("Project budgets", "SELECT p.name, p.budget, p.status, d.department_name\nFROM projects p\nJOIN departments d ON p.dept_id = d.id\nORDER BY p.budget DESC"),
]

HELP_TEXT = """\
[bold cyan]CSVQL[/] — SQL query engine over CSV files
 
[bold]Keybindings[/]
  [yellow]F5[/] / [yellow]Ctrl+R[/]       Run query
  [yellow]Tab[/] / [yellow]Enter[/]        Accept autocomplete suggestion
  [yellow]Escape[/]             Dismiss autocomplete
  [yellow]Ctrl+L[/]             Clear editor
  [yellow]Ctrl+H[/]             Toggle this help
  [yellow]Ctrl+Q[/]             Quit
 
[bold]Autocomplete[/]
  Suggestions appear as you type.
  Shows SQL keywords, table names, and column names.
  Press Tab or Enter to insert the selected suggestion.
 
[bold]Supported SQL[/]
  SELECT · FROM · WHERE · JOIN (INNER/LEFT/RIGHT/FULL)
  GROUP BY · HAVING · ORDER BY · LIMIT · OFFSET
  COUNT · SUM · AVG · MIN · MAX
  CREATE TABLE · ALTER TABLE (ADD/DROP COLUMN)
  INSERT INTO · UPDATE · DELETE
 
[bold]File conventions[/]
  Tables are CSV files in the working directory.
  Table name = filename without .csv
  e.g.  employees → ./employees.csv
 
[bold]Examples[/]
  SELECT * FROM orders WHERE amount > 100
  SELECT dept, AVG(salary) FROM salaries GROUP BY dept
  INSERT INTO orders (id, item) VALUES (1, 'book')
"""


# ── Help Screen ──────────────────────────────────────────────────────────────

class HelpScreen(ModalScreen):
    BINDINGS = [Binding("escape,ctrl+h", "dismiss", "Close")]

    def compose(self) -> ComposeResult:
        yield Static(HELP_TEXT, classes="help-content")

    def on_key(self, event: events.Key):
        if event.key in ("escape", "ctrl+h"):
            self.dismiss()


# ── Schema Panel ─────────────────────────────────────────────────────────────

class SchemaPanel(ScrollableContainer):
    DEFAULT_CSS = """
    SchemaPanel {
        width: 28;
        border-right: solid $primary-darken-2;
        padding: 0 1;
        background: $surface-darken-1;
    }
    SchemaPanel .schema-title {
        text-style: bold;
        color: $primary;
        margin-top: 1;
    }
    SchemaPanel .table-name {
        color: $accent;
        text-style: bold;
        margin-top: 1;
    }
    SchemaPanel .col-name {
        color: $text-muted;
        padding-left: 2;
    }
    """

    def __init__(self, csv_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.csv_dir = csv_dir

    def compose(self) -> ComposeResult:
        yield Label("Tables", classes="schema-title")
        yield Label("", id="schema-body")

    def refresh_schema(self):
        body = self.query_one("#schema-body", Label)
        lines = []
        csv_files = sorted(Path(self.csv_dir).glob("*.csv"))
        if not csv_files:
            body.update("[dim]No CSV files found[/]")
            return

        for csv_path in csv_files:
            table_name = csv_path.stem
            lines.append(f"[bold cyan]▸ {table_name}[/]")
            try:
                with open(csv_path, newline="") as f:
                    reader = csv.reader(f)
                    headers = next(reader, [])
                for h in headers:
                    lines.append(f"  [dim]· {h.strip()}[/]")
            except Exception as e:
                lines.append(f"  [dim]· Error reading file: {e}[/]")
        body.update("\n".join(lines))


# ── Autocomplete Dropdown ─────────────────────────────────────────────────────

class AutocompleteDropdown(Static):
    """
    A floating autocomplete popup that appears below the editor.
    Shows filtered SQL keyword / table / column suggestions.
    """
    DEFAULT_CSS = """
    AutocompleteDropdown {
        layer: overlay;
        background: $surface;
        border: solid $accent;
        height: auto;
        max-height: 10;
        width: 36;
        display: none;
        padding: 0;
    }
    AutocompleteDropdown ListView {
        height: auto;
        max-height: 10;
        background: $surface;
        padding: 0;
    }
    AutocompleteDropdown ListItem {
        padding: 0 1;
        height: 1;
    }
    AutocompleteDropdown ListItem:hover {
        background: $primary-darken-2;
    }
    AutocompleteDropdown ListItem.--highlight {
        background: $accent;
        color: $text;
    }
    """

    def compose(self) -> ComposeResult:
        yield ListView(id="ac-list")

    def show_suggestions(self, suggestions: List[Tuple[str, str]]):
        """Update the list with (display_text, insert_value) tuples and make visible."""
        lv = self.query_one("#ac-list", ListView)
        lv.clear()
        for display, _ in suggestions:
            # colour-code by type
            if display.startswith("⌗ "):          # column
                label = Text(display, style="cyan")
            elif display.startswith("◈ "):         # table
                label = Text(display, style="yellow")
            else:                                   # keyword
                label = Text(display, style="green")
            lv.append(ListItem(Static(label)))
        self.display = True
        # select first item
        if lv.children:
            lv.index = 0

    def hide(self):
        self.display = False
        lv = self.query_one("#ac-list", ListView)
        lv.clear()

    def move_down(self):
        lv = self.query_one("#ac-list", ListView)
        if lv.index is None:
            lv.index = 0
        elif lv.index < len(lv.children) - 1:
            lv.index += 1

    def move_up(self):
        lv = self.query_one("#ac-list", ListView)
        if lv.index is not None and lv.index > 0:
            lv.index -= 1

    def selected_index(self) -> Optional[int]:
        lv = self.query_one("#ac-list", ListView)
        return lv.index


# ── Query Editor with autocomplete logic ────────────────────────────────────

class QueryEditor(TextArea):
    DEFAULT_CSS = """
    QueryEditor {
        height: 20;
        border: solid $primary-darken-2;
        background: $surface;
    }
    QueryEditor:focus {
        border: solid $accent;
    }
    """


# ── Status Bar ───────────────────────────────────────────────────────────────

class StatusBar(Static):
    DEFAULT_CSS = """
    StatusBar {
        height: 1;
        background: $primary-darken-3;
        color: $text;
        padding: 0 2;
    }
    """

    def set_ok(self, rows: int, elapsed: float, cols: int):
        self.update(
            f"[green]✓[/] {rows} row(s) · {cols} column(s) · {elapsed * 1000:.1f}ms"
        )

    def set_error(self, msg: str):
        self.update(f"[red]✗ {msg}[/]")

    def set_info(self, msg: str):
        self.update(f"[yellow]ℹ {msg}[/]")


# ── Results Table ────────────────────────────────────────────────────────────

class ResultsTable(DataTable):
    DEFAULT_CSS = """
    ResultsTable {
        height: 1fr;
        border: solid $primary-darken-2;
    }
    ResultsTable:focus {
        border: solid $accent;
    }
    """

    def on_click(self, event: events.Click) -> None:
        # Textual crashes with IndexError when you click the header of an empty
        # DataTable (ordered_columns is empty but column_index is 0).
        # Stop the event before it propagates to DataTable's handler.
        if len(self.columns) == 0:
            event.stop()

# ── Main Application ─────────────────────────────────────────────────────────

class CSVQLApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #main-area {
        layout: horizontal;
        height: 1fr;
    }
    #right-pane {
        layout: vertical;
        width: 1fr;
    }
    #editor-label {
        background: $primary-darken-3;
        color: $accent;
        padding: 0 2;
        height: 1;
    }
    #results-label {
        background: $primary-darken-3;
        color: $accent;
        padding: 0 2;
        height: 1;
    }
    #editor-wrapper {
        height: auto;
        width: 1fr;
        layout: vertical;
    }
    AutocompleteDropdown {
        dock: bottom;
        height: auto;
        max-height: 10;
    }
    """

    BINDINGS = [
        Binding("f5",         "run_query", "Run"),
        Binding("ctrl+r",     "run_query", "Run",    show=False),  # mac f5 alt
        Binding("ctrl+enter", "run_query", "Run",    show=False),  # alt run
        Binding("ctrl+l",     "clear",     "Clear"),
        Binding("ctrl+h",     "help",      "Help"),               # real terminal
        Binding("f1",         "help",      "Help",   show=False),  # vs code safe
        Binding("ctrl+q",     "quit",      "Quit"),               # real terminal
        Binding("ctrl+x",     "quit",      "Quit",   show=False),  # vs code safe
        Binding("f10",        "quit",      "Quit",   show=False),  # vs code safe
    ]
    TITLE = f"CSVQL v{VERSION}"

    # reactive list of current suggestions: [(display, insert_value)]
    _suggestions: List[Tuple[str, str]] = []
    _ac_visible: bool = False

    def __init__(self, csv_dir: str = "."):
        super().__init__()
        self.csv_dir = os.path.abspath(csv_dir)
        self.executor = Executor(csv_dir=self.csv_dir)
        # Build the initial completion corpus from CSV files
        self._schema: Dict[str, List[str]] = {}   # table_name -> [col, ...]
        self._refresh_schema_cache()

    # ── Schema helpers ────────────────────────────────────────────────────

    def _refresh_schema_cache(self):
        """Scan csv_dir and build a dict of table -> columns for autocomplete."""
        self._schema = {}
        for csv_path in sorted(Path(self.csv_dir).glob("*.csv")):
            try:
                with open(csv_path, newline="") as f:
                    reader = csv.reader(f)
                    headers = next(reader, [])
                self._schema[csv_path.stem] = [h.strip() for h in headers]
            except Exception:
                self._schema[csv_path.stem] = []

    def _build_completions(self, prefix: str) -> List[Tuple[str, str]]:
        """
        Return up to 12 (display, insert) pairs for a given lowercase prefix.
        Priority: tables > columns > keywords.
        """
        prefix_up = prefix.upper()
        results: List[Tuple[str, str]] = []

        # Tables
        for tbl in sorted(self._schema.keys()):
            if tbl.lower().startswith(prefix.lower()):
                results.append((f"◈ {tbl}", tbl))

        # Columns (qualified: table.column)
        for tbl, cols in self._schema.items():
            for col in cols:
                if col.lower().startswith(prefix.lower()):
                    results.append((f"⌗ {col}", col))

        # SQL keywords
        for kw in SQL_KEYWORDS:
            if kw.upper().startswith(prefix_up):
                results.append((kw, kw))

        # deduplicate by insert value, preserve order
        seen = set()
        deduped = []
        for item in results:
            if item[1] not in seen:
                seen.add(item[1])
                deduped.append(item)

        return deduped[:12]

    def _current_word(self, text: str) -> str:
        """Extract the word the cursor is currently on / just completed."""
        # grab only word chars and dots (e.g. "table.col" prefix)
        m = re.search(r'[\w.]+$', text)
        return m.group(0) if m else ""

    # ── Widget tree ───────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main-area"):
            yield SchemaPanel(self.csv_dir, id="schema-panel")
            with Vertical(id="right-pane"):
                yield Label("  ✎  SQL EDITOR  [dim](F5 or Ctrl+R to run · Tab to autocomplete)[/]", id="editor-label")
                yield QueryEditor(
                    "SELECT * FROM employees LIMIT 10",
                    language="sql",
                    id="editor",
                )
                yield AutocompleteDropdown(id="autocomplete")
                yield Label("  ⊞  RESULTS", id="results-label")
                yield ResultsTable(
                    id="results",
                    zebra_stripes=True,
                    cursor_type="row",
                )
        yield StatusBar("Ready — press F5 or Ctrl+R to run a query", id="status")
        yield Footer()

    def on_mount(self):
        self.query_one("#schema-panel", SchemaPanel).refresh_schema()
        self.query_one("#editor", QueryEditor).focus()

    # ── Autocomplete event wiring ─────────────────────────────────────────

    def on_text_area_changed(self, event: TextArea.Changed) -> None:
        """Fires every time the editor content changes — drive autocomplete."""
        editor = self.query_one("#editor", QueryEditor)
        ac = self.query_one("#autocomplete", AutocompleteDropdown)

        # Get text up to the cursor
        cursor_row, cursor_col = editor.cursor_location
        lines = editor.text.split("\n")
        if cursor_row >= len(lines):
            ac.hide()
            self._ac_visible = False
            return

        text_up_to_cursor = lines[cursor_row][:cursor_col]
        word = self._current_word(text_up_to_cursor)

        if len(word) < 1:
            ac.hide()
            self._ac_visible = False
            self._suggestions = []
            return

        suggestions = self._build_completions(word)
        self._suggestions = suggestions

        if suggestions:
            ac.show_suggestions(suggestions)
            self._ac_visible = True
        else:
            ac.hide()
            self._ac_visible = False

    def on_key(self, event: events.Key) -> None:
        """Intercept Tab / Enter / arrow keys for autocomplete navigation."""
        ac = self.query_one("#autocomplete", AutocompleteDropdown)

        if not self._ac_visible:
            return

        if event.key == "escape":
            ac.hide()
            self._ac_visible = False
            event.prevent_default()
            event.stop()

        elif event.key in ("tab", "enter") and self._suggestions:
            idx = ac.selected_index()
            if idx is not None and idx < len(self._suggestions):
                self._accept_suggestion(self._suggestions[idx][1])
            ac.hide()
            self._ac_visible = False
            event.prevent_default()
            event.stop()

        elif event.key == "down":
            ac.move_down()
            event.prevent_default()
            event.stop()

        elif event.key == "up":
            ac.move_up()
            event.prevent_default()
            event.stop()

    def _accept_suggestion(self, insert_value: str):
        """Replace the current partial word in the editor with insert_value."""
        editor = self.query_one("#editor", QueryEditor)
        cursor_row, cursor_col = editor.cursor_location
        lines = editor.text.split("\n")
        if cursor_row >= len(lines):
            return

        line = lines[cursor_row]
        text_before = line[:cursor_col]
        text_after = line[cursor_col:]

        # find the partial word that was typed
        m = re.search(r'[\w.]+$', text_before)
        if m:
            partial_start = m.start()
            new_before = text_before[:partial_start] + insert_value
        else:
            new_before = text_before + insert_value

        lines[cursor_row] = new_before + text_after
        new_text = "\n".join(lines)

        # Replace content and reposition cursor
        editor.load_text(new_text)
        new_col = len(new_before)
        editor.move_cursor((cursor_row, new_col))

    # ── Actions ───────────────────────────────────────────────────────────

    def action_run_query(self):
        # Hide autocomplete before running
        ac = self.query_one("#autocomplete", AutocompleteDropdown)
        ac.hide()
        self._ac_visible = False

        editor = self.query_one("#editor", QueryEditor)
        sql = editor.text.strip()
        if not sql:
            self.query_one("#status", StatusBar).set_info("No query to run")
            return
        self._execute(sql)

    def action_clear(self):
        ac = self.query_one("#autocomplete", AutocompleteDropdown)
        ac.hide()
        self._ac_visible = False
        self.query_one("#editor", QueryEditor).clear()
        self.query_one("#results", ResultsTable).clear(columns=True)
        self.query_one("#status", StatusBar).set_info("Editor cleared")

    def action_help(self):
        self.push_screen(HelpScreen())

    # ── Execution pipeline ────────────────────────────────────────────────

    def _execute(self, sql: str):
        status = self.query_one("#status", StatusBar)
        table  = self.query_one("#results", ResultsTable)
        table.clear(columns=True)
        start = time.perf_counter()

        try:
            ast = parse(sql)
            execution_plan = plan(ast)
            executor = Executor(csv_dir=self.csv_dir)
            cols, rows = executor.run(execution_plan)
            elapsed = time.perf_counter() - start

            self._populate_table(table, cols, rows)
            status.set_ok(len(rows), elapsed, len(cols))

            # Refresh schema after DDL queries
            self.query_one("#schema-panel", SchemaPanel).refresh_schema()
            self._refresh_schema_cache()

        except LexError as e:
            status.set_error(f"Syntax error: {e}")
        except ParseError as e:
            status.set_error(f"Parse error: {e}")
        except ExecutionError as e:
            status.set_error(f"Execution error: {e}")
        except Exception as e:
            status.set_error(f"Unexpected error: {e}")

    def _populate_table(self, table: ResultsTable, cols: List[str], rows: List[dict]):
        display_cols = cols if cols else []

        if not display_cols and rows:
            seen = set()
            deduped = []
            for k in rows[0].keys():
                bare = k.split(".", 1)[1] if "." in k else k
                if bare not in seen:
                    seen.add(bare)
                    deduped.append(bare)
            display_cols = deduped

        if not display_cols:
            return

        for col in display_cols:
            table.add_column(col, key=col)

        for row in rows:
            cells = []
            for col in display_cols:
                val = row.get(col)
                if val is None:
                    for k, v in row.items():
                        bare = k.split(".", 1)[1] if "." in k else k
                        if bare == col:
                            val = v
                            break
                cells.append("" if val is None else str(val))
            table.add_row(*cells)


if __name__ == "__main__":
    app = CSVQLApp(csv_dir="data")
    app.run()
