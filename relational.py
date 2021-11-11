from typing import Dict, List, Tuple
import pyodbc


class RelationalDatabase:
    def __init__(
        self, server: str, database: str, username: str, password: str
    ) -> None:
        self.conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + server
            + ";DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
        )

        self.cursor = self.conn.cursor()

    def query_all(self, columns: List[str], table: str) -> List[List[str]]:
        result = []

        self.cursor.execute(f"SELECT {', '.join(columns)} FROM dbo.{table};")
        row = self.cursor.fetchone()

        while row:
            result.append(row)
            row = self.cursor.fetchone()

        return result
