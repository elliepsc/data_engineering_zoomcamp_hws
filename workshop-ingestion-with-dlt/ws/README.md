# Workshop — AI-Assisted Data Ingestion with dlt

Data Engineering Zoomcamp 2026 | From APIs to Warehouses with dlt + DuckDB

## Objectif

Construire un pipeline de données complet avec un IDE assisté par IA (GitHub Copilot + MCP dlt), en ingérant des données depuis l'[Open Library API](https://openlibrary.org/developers/api) vers une base DuckDB locale — sans écrire de code manuellement.

## Stack

- Python 3.12
- dlt 1.22.1
- DuckDB
- uv (package manager)
- GitHub Copilot + dlt MCP Server

## Project Structure

```
ws/
├── .dlt/                          # dlt config
├── .github/
│   └── instructions/              # Copilot instructions générées par dlt init
│       ├── build-rest-api.instructions.md
│       ├── dlt.instructions.md
│       ├── rest_api_extract_parameters.instructions.md
│       └── rest_api_pagination.instructions.md
├── .venv/                         # Virtual environment (gitignored)
├── .vscode/
│   └── mcp.json                   # dlt MCP server config pour Copilot
├── .gitignore
├── requirements.txt
├── open_library_pipeline.py       # Pipeline principal
├── open_library_pipeline.duckdb   # Base DuckDB locale (gitignored)
└── README.md
```

## Setup

### 1. Créer et activer l'environnement virtuel

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Installer les dépendances

```bash
pip install "dlt[duckdb]" "dlt[workspace]"
```

### 3. Initialiser le projet dlt

```bash
dlt init dlthub:open_library duckdb
```

Cette commande scaffolde les fichiers de configuration et les instructions Copilot dans `.github/instructions/`.

### 4. Lancer le pipeline

```bash
python open_library_pipeline.py
```

Le pipeline :
- Interroge l'Open Library Search API avec la requête `q=python`
- Pagine via offset (100 records/page) jusqu'à `numFound` total
- Charge 6,349 livres dans DuckDB en ~40s

### 5. Inspecter les données

```bash
dlt pipeline open_library_pipeline show
```

Ouvre le dashboard dlt sur http://localhost:2718

## Source de données

| Propriété | Valeur |
|-----------|--------|
| API | Open Library Search API |
| Base URL | https://openlibrary.org |
| Endpoint | `/search.json` |
| Paramètres | `q=python`, `limit=100` |
| Data selector | `docs` |
| Pagination | Offset (`numFound` = 6,349) |
| Auth | Aucune |

## Pipeline Configuration

```python
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://openlibrary.org",
    },
    "resources": [
        {
            "name": "books",
            "endpoint": {
                "path": "search.json",
                "params": {"q": "python", "limit": 100},
                "data_selector": "docs",
                "paginator": {
                    "type": "offset",
                    "limit": 100,
                    "offset_param": "offset",
                    "limit_param": "limit",
                    "total_path": "numFound",
                },
            },
        },
    ],
}
```

## Résultats

| Metric | Valeur |
|--------|--------|
| Livres chargés | 6,349 |
| Temps total | ~41s |
| Extract | 38.9s |
| Normalize | 524ms |
| Load | 1.7s |

## MCP Server Setup

Le dlt MCP Server donne à Copilot accès à la documentation dlt et aux métadonnées du pipeline en temps réel.

Configuration dans `.vscode/mcp.json` :

```json
{
  "servers": {
    "dlt": {
      "command": "uv",
      "args": [
        "run",
        "--with", "dlt[duckdb]",
        "--with", "dlt-mcp[search]",
        "python", "-m", "dlt_mcp"
      ]
    }
  }
}
```

## Notes techniques

**Pagination** : L'API Open Library retourne `numFound` dans la réponse — dlt peut calculer exactement le nombre de pages à parcourir via `total_path="numFound"`, contrairement au taxi pipeline qui utilisait `stop_after_empty_page=True`.

**Fix paginator** : `OffsetPaginatorConfig` requiert le champ `limit` explicitement dans la config dict, même si `limit_param` est défini.

**Normalisation** : dlt crée automatiquement des tables enfants pour les champs de type liste (`author_name`, `author_key`, `ia`, etc.), résultant en plusieurs tables dans `open_library_pipeline_dataset`.
