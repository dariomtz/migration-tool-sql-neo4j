import csv
from os import path
from typing import List
from graph import NeoDatabase
from relational import RelationalDatabase

csv_dir = "C:/Users/Dario/.Neo4jDesktop/relate-data/dbmss/dbms-aeba5e3b-3b17-4514-9781-2049d5499f59/import"

tables = {
    "Articulo": {
        "name": "DIM_ARTICULO",
        "id": "IDArticulo",
        "columns": {
            "IDArticulo": "toInteger",
            "Descripcion": "",
            "Codigo": "",
            "Color": "",
            "UnidadDeMedida": "",
            "DescripcionGrupo": "",
            "DescripcionTipo": "",
        },
    },
    "Almacen": {
        "name": "DIM_ALMACEN",
        "id": "IDAlmacen",
        "columns": {
            "IDAlmacen": "toInteger",
            "Descripcion": "",
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
    "Salidas": {
        "name": "DIM_SALIDAS",
        "id": "IDSalidas",
        "columns": {
            "IDSalidas": "toInteger",
        },
    },
    "Tiempo": {
        "name": "DIM_TIME",
        "id": "IDTiempo",
        "columns": {
            "IDTiempo": "toInteger",
            "Año": "toInteger",
            "Semestre": "toInteger",
            "Trimestre": "toInteger",
            "Mes": "toInteger",
            "MesLetra": "",
            "DiaSemana": "",
            "DiaMes": "toInteger",
            "fecha": "",
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

facts = {
    "name": "FACTS",
    "columns": {
        "IDVendedor": "toInteger",
        "IDCliente": "toInteger",
        "IDSalidas": "toInteger",
        "IDAlmacen": "toInteger",
        "IDArticulo": "toInteger",
        "IDTiempo": "toInteger",
    },
}

relationships = [
    ("Salidas", "Vendedor", "vendida_por"),
    ("Salidas", "Cliente", "vendida_a"),
    ("Salidas", "Tiempo", "ocurrio_en"),
    ("Salidas", "Almacen", "salio_de"),
    ("Articulo", "Salidas", "salio_en"),
]


def create_csv(file: str, columns: List[str], rows: List[str]):
    with open(file, "w", newline="") as csvfile:
        spamwritter = csv.writer(
            csvfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
        )
        spamwritter.writerow(columns)
        spamwritter.writerows(rows)


if __name__ == "__main__":
    db = RelationalDatabase(
        "DESKTOP-K47GUIA\DEVELOPER", "DIM720956", "sa", "nicestdatabase"
    )
    neo = NeoDatabase("bolt://localhost:7687", "neo4j", "nicestdatabase")

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

    rows = db.query_all(facts["columns"].keys(), facts["name"])
    create_csv(path.join(csv_dir, "Facts.csv"), facts["columns"].keys(), rows)

    for rel in relationships:
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

        neo.run_query(rel_query, {"filename": f"file:///Facts.csv"})
