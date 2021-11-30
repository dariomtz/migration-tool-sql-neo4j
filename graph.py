from typing import Dict
from neo4j import GraphDatabase


class NeoDatabase:
    def __init__(self, uri, user, password) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    @staticmethod
    def create_load_query(name: str, columns: Dict[str, str]) -> str:
        prop_settings = ", ".join(
            [f"n.{col} = {func}(row.{col})" for col, func in columns.items()]
        )
        return f"LOAD CSV WITH HEADERS FROM $filename AS row \
                CREATE (n:{name}) SET n = row, {prop_settings}"

    @staticmethod
    def create_index_query(name: str, column: str):
        return f"CREATE INDEX index_{name} FOR (n:{name}) ON (n.{column})"

    def create_rel_query(
        self,
        rel_name: str,
        node1: str,
        prop1: str,
        func1: str,
        node2: str,
        prop2: str,
        func2: str,
    ):

        return f"LOAD CSV WITH HEADERS FROM $filename AS row\
                MATCH (n:{node1}) \
                MATCH (m:{node2}) \
                WHERE n.{prop1} = {func1}(row.{prop1}) \
                AND m.{prop2} = {func2}(row.{prop2}) \
                CREATE (n) - [:{rel_name}] -> (m)"

    def run_query(self, query: str, *args, **properties) -> str:
        return self.driver.session().run(query, **properties)
