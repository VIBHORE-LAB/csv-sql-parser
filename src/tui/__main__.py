from src.tui import CSVQLApp

if __name__ == "__main__":
    app = CSVQLApp(csv_dir="data")
    app.run()
