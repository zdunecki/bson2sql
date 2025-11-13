# BSON to SQL Converter

Convert MongoDB BSON exports to SQL import scripts using a schema definition.

## Features
- Parses BSON files and generates SQL scripts for PostgreSQL, MySQL, or SQLite
- Supports custom schema mapping and type conversion
- Handles array fields and nested objects
- CLI usage for easy integration

## Installation

```bash
bun install
```

## Usage

```bash
bun run build
node ./dist/index.js <bson_file> <schema_file.json> [database_type]
```

- `<bson_file>`: Path to the BSON file exported from MongoDB
- `<schema_file.json>`: Path to the schema definition JSON file
- `[database_type]`: Optional, one of `mysql`, `postgresql`, or `sqlite` (default: `mysql`)

### Example

```bash
node ./dist/index.js data.bson schema.json mysql
```

## Example Schema

```json
{
	"tables": {
		"users": {
			"primary_key": "id",
			"columns": {
				"id": "id",
				"username": "string",
				"email": "string",
				"created_at": "datetime",
				"is_active": "boolean",
				"profile_data": "json"
			},
			"not_null": ["username", "email"],
			"field_mapping": {
				"id": "_id",
				"username": "username",
				"email": "email",
				"created_at": "createdAt",
				"is_active": "active",
				"profile_data": "profile"
			}
		}
	}
}
```

## License
MIT
# bson2sql