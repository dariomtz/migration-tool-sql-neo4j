import csv
from os import path
from typing import List
from graph import NeoDatabase
from relational import RelationalDatabase

tables = {
    "Articulo": {
        "name": "DIM_ARTICULO",
        "id": "IDArticulo",
        "columns": {
            "IDArticulo": "toInteger",
            "Nombre": "",
            "Codigo": "",
            "ColorBase": "",
            "ColorDerivado": "",
            "UnidadDeMedida": "",
            "DescripcionGrupo": "",
            "DescripcionTipo": "",
        },
    },
    "Cliente": {
        "name": "DIM_CLIENTE",
        "id": "IDCliente",
        "columns": {
            "IDCliente": "toInteger",
            "RazonSocial": "",
            "Colonia": "",
            "CodigoPostal": "",
            "Estado": "",
        },
    },
    "Comprador": {
        "name": "DIM_COMPRADOR",
        "id": "IDComprador",
        "columns": {"IDComprador": "toInteger", "Nombre": ""},
    },
    "FacturaCompras": {
        "name": "DIM_FACTURA_C",
        "id": "IDFactura",
        "columns": {
            "IDFactura": "toInteger",
            "Folio": "toInteger",
            "Transporte": "",
            "CondicionPago": "",
        },
    },
    "FacturaVentas": {
        "name": "DIM_FACTURA_V",
        "id": "IDFactura",
        "columns": {
            "IDFactura": "toInteger",
            "Folio": "toInteger",
            "Transporte": "",
            "CondicionPago": "",
        },
    },
    "Proveedor": {
        "name": "DIM_PROVEEDOR",
        "id": "IDProveedor",
        "columns": {
            "IDProveedor": "toInteger",
            "RazonSocial": "",
            "Nombre": "",
            "Colonia": "",
            "CodigoPostal": "",
            "Ciudad": "",
            "Estado": "",
            "Pais": "",
        },
    },
    "Tiempo": {
        "name": "DIM_TIEMPO",
        "id": "IDTime",
        "columns": {
            "IDTime": "toInteger",
            "Año": "toInteger",
            "Semestre": "toInteger",
            "Trimestre": "toInteger",
            "Mes": "toInteger",
            "MesLetra": "",
            "DiaSemana": "",
            "DiaMes": "toInteger",
        },
    },
    "Vendedor": {
        "name": "DIM_VENDEDOR",
        "id": "IDVendedor",
        "columns": {
            "IDVendedor": "toInteger",
            "Nombre": "",
        },
    },
}

facts_tables = [
    {
        "name": "FACTS_COMPRAS",
        "columns": {
            "IDTime": "toInteger",
            "IDArticulo": "toInteger",
            "IDComprador": "toInteger",
            "IDProveedor": "toInteger",
            "IDFactura": "toInteger",
        },
        "relationships": [
            ("FacturaCompras", "Comprador", "comprada_por"),
            ("Proveedor", "FacturaCompras", "proveedor_de"),
            ("FacturaCompras", "Tiempo", "ocurrio_en"),
            ("Articulo", "FacturaCompras", "comprado_en"),
        ],
    },
    {
        "name": "FACTS_VENTAS",
        "columns": {
            "IDTime": "toInteger",
            "IDVendedor": "toInteger",
            "IDArticulo": "toInteger",
            "IDCliente": "toInteger",
            "IDFactura": "toInteger",
        },
        "relationships": [
            ("FacturaVentas", "Cliente", "vendida_a"),
            ("FacturaVentas", "Vendedor", "vendida_por"),
            ("FacturaVentas", "Tiempo", "ocurrio_en"),
            ("Articulo", "FacturaVentas", "vendido_en"),
        ],
    },
]


def create_csv(file: str, columns: List[str], rows: List[str]):
    with open(file, "w", newline="") as csvfile:
        spamwritter = csv.writer(
            csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        spamwritter.writerow(columns)
        spamwritter.writerows(rows)


if __name__ == "__main__":
    print("Information for SQL Server Database")
    db = RelationalDatabase(
        input("Server: "), input("Database: "), input("Login: "), input("Password: ")
    )

    print("Make sure your Neo4j database is running.")
    user = input("User: [neo4j]")
    neo = NeoDatabase(
        "bolt://localhost:7687", "neo4j" if not user else user, input("Password: ")
    )

    csv_dir = input("Imports folder from neo4j database: ")
    if not path.exists(csv_dir):
        print("Error in the given path.")

    count = 1

    for name, table in tables.items():
        rows = db.query_all(table["columns"].keys(), table["name"])
        filename = path.join(csv_dir, name + ".csv")
        create_csv(
            filename,
            table["columns"].keys(),
            rows,
        )

        load_query = neo.create_load_query(name, table["columns"])
        neo.run_query(load_query, {"filename": f"file:///{name}.csv"})

        index_query = neo.create_index_query(name, table["id"])

    for facts in facts_tables:
        rows = db.query_all(facts["columns"].keys(), facts["name"])
        create_csv(
            path.join(csv_dir, f"{facts['name']}.csv"), facts["columns"].keys(), rows
        )

        for rel in facts["relationships"]:
            col1, col2, rel_name = rel
            id1 = tables[col1]["id"]
            id2 = tables[col2]["id"]
            rel_query = neo.create_rel_query(
                rel_name,
                col1,
                id1,
                tables[col1]["columns"][id1],
                col2,
                id2,
                tables[col2]["columns"][id2],
            )

            neo.run_query(rel_query, {"filename": f"file:///{facts['name']}.csv"})

    print("Migration ran succesfully!")
