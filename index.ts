#!/usr/bin/env node
/**
 * BSON to SQL Converter (TypeScript)
 * Converts MongoDB BSON exports to SQL import scripts based on a schema definition.
 */

import * as fs from "fs";
import * as BSON from "bson";

interface ColumnDefinition {
  [columnName: string]: string;
}

interface TableConfig {
  primary_key?: string;
  columns: ColumnDefinition;
  not_null?: string[];
  field_mapping: { [sqlColumn: string]: string };
}

interface SchemaConfig {
  tables: {
    [tableName: string]: TableConfig;
  };
}

type DatabaseType = "postgresql" | "mysql" | "sqlite";

class BSONToSQLConverter {
  private schema: SchemaConfig;
  private dbType: DatabaseType;

  constructor(schemaConfig: SchemaConfig, dbType: DatabaseType = "mysql") {
    this.schema = schemaConfig;
    this.dbType = dbType;
  }

  /**
   * Convert BSON file to SQL import script
   */
  convertBSONFile(bsonFilePath: string): string {
    // Read BSON file
    const bsonData = fs.readFileSync(bsonFilePath);

    // Parse BSON documents
    const documents: any[] = [];
    let offset = 0;

    while (offset < bsonData.length) {
      try {
        const docSize = bsonData.readInt32LE(offset);
        const docBytes = bsonData.slice(offset, offset + docSize);
        const doc = BSON.deserialize(docBytes);
        documents.push(doc);
        offset += docSize;
      } catch (e) {
        console.error(
          `Warning: Error parsing document at offset ${offset}:`,
          e
        );
        break;
      }
    }

    console.error(`Parsed ${documents.length} documents from BSON file`);

    // Generate SQL
    return this.generateSQL(documents);
  }

  /**
   * Generate complete SQL script from documents
   */
  private generateSQL(documents: any[]): string {
    const sqlParts: string[] = [];

    // Add header comment
    sqlParts.push(`-- Generated SQL import script`);
    sqlParts.push(`-- Source: MongoDB BSON export`);
    sqlParts.push(`-- Target: ${this.dbType.toUpperCase()}`);
    sqlParts.push(`-- Generated: ${new Date().toISOString()}`);
    sqlParts.push(`-- Documents processed: ${documents.length}`);
    sqlParts.push("");

    // Generate CREATE TABLE statements
    sqlParts.push("-- Table creation");
    for (const [tableName, tableConfig] of Object.entries(this.schema.tables)) {
      sqlParts.push(this.createTableStatement(tableName, tableConfig));
      sqlParts.push("");
    }

    // Generate INSERT statements
    sqlParts.push("-- Data insertion");
    sqlParts.push("BEGIN;");
    sqlParts.push("");

    for (const doc of documents) {
      const inserts = this.generateInsertsForDocument(doc);
      sqlParts.push(...inserts);
    }

    sqlParts.push("");
    sqlParts.push("COMMIT;");

    return sqlParts.join("\n");
  }

  /**
   * Generate CREATE TABLE statement
   */
  private createTableStatement(
    tableName: string,
    tableConfig: TableConfig
  ): string {
    const lines: string[] = [`CREATE TABLE IF NOT EXISTS ${tableName} (`];

    const columns: string[] = [];
    for (const [colName, colType] of Object.entries(tableConfig.columns)) {
      let colDef = `    ${colName} ${this.mapType(colType)}`;

      // Add constraints
      if (colName === tableConfig.primary_key) {
        colDef += " PRIMARY KEY";
      }
      if (tableConfig.not_null?.includes(colName)) {
        colDef += " NOT NULL";
      }

      columns.push(colDef);
    }

    lines.push(columns.join(",\n"));
    lines.push(");");

    return lines.join("\n");
  }

  /**
   * Map generic type to database-specific type
   */
  private mapType(typeStr: string): string {
    const typeMapping: Record<DatabaseType, Record<string, string>> = {
      postgresql: {
        id: "SERIAL",
        string: "TEXT",
        int: "INTEGER",
        bigint: "BIGINT",
        float: "DOUBLE PRECISION",
        decimal: "DECIMAL",
        boolean: "BOOLEAN",
        date: "DATE",
        datetime: "TIMESTAMP",
        timestamp: "TIMESTAMP",
        json: "JSONB",
        text: "TEXT",
      },
      mysql: {
        id: "INT AUTO_INCREMENT",
        string: "VARCHAR(255)",
        int: "INT",
        bigint: "BIGINT",
        float: "DOUBLE",
        decimal: "DECIMAL",
        boolean: "BOOLEAN",
        date: "DATE",
        datetime: "DATETIME",
        timestamp: "TIMESTAMP",
        json: "JSON",
        text: "TEXT",
      },
      sqlite: {
        id: "INTEGER PRIMARY KEY AUTOINCREMENT",
        string: "TEXT",
        int: "INTEGER",
        bigint: "INTEGER",
        float: "REAL",
        decimal: "REAL",
        boolean: "INTEGER",
        date: "TEXT",
        datetime: "TEXT",
        timestamp: "TEXT",
        json: "TEXT",
        text: "TEXT",
      },
    };

    return typeMapping[this.dbType]?.[typeStr] || "TEXT";
  }

  /**
   * Generate INSERT statements for a document
   */
  private generateInsertsForDocument(doc: any): string[] {
    const inserts: string[] = [];

    for (const [tableName, tableConfig] of Object.entries(this.schema.tables)) {
      // Check if this table has array fields (like pages[])
      const hasArrayFields = Object.values(tableConfig.field_mapping).some(
        (field) => field.includes("[]")
      );

      if (hasArrayFields) {
        // Handle array fields - generate multiple inserts
        const arrayInserts = this.generateArrayInserts(doc, tableName, tableConfig);
        inserts.push(...arrayInserts);
      } else {
        // Regular single-row insert
        const values: Record<string, any> = {};

        for (const [sqlColumn, mongoField] of Object.entries(
          tableConfig.field_mapping
        )) {
          const value = this.extractFieldValue(doc, mongoField);
          if (value !== null && value !== undefined) {
            values[sqlColumn] = value;
          }
        }

        // Skip if no values
        if (Object.keys(values).length === 0) {
          continue;
        }

        // Generate INSERT
        const columns = Object.keys(values).join(", ");
        const placeholders = Object.entries(values)
          .map(([col, v]) => this.formatValue(v, col))
          .join(", ");

        const insertSQL = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders});`;
        inserts.push(insertSQL);
      }
    }

    return inserts;
  }

  /**
   * Generate INSERT statements for array fields (e.g., pages[])
   */
  private generateArrayInserts(
    doc: any,
    tableName: string,
    tableConfig: TableConfig
  ): string[] {
    const inserts: string[] = [];

    // Find the array field path (e.g., "pages")
    let arrayPath = "";
    for (const [sqlColumn, mongoField] of Object.entries(
      tableConfig.field_mapping
    )) {
      if (mongoField.includes("[]")) {
        arrayPath = mongoField.split("[")[0];
        break;
      }
    }

    if (!arrayPath) {
      return inserts;
    }

    // Extract the array
    const array = this.extractFieldValue(doc, arrayPath);
    if (!Array.isArray(array) || array.length === 0) {
      return inserts;
    }

    // Extract parent field (e.g., urlId for crawler_pages)
    let parentField = "";
    for (const [sqlColumn, mongoField] of Object.entries(
      tableConfig.field_mapping
    )) {
      if (!mongoField.includes("[]") && sqlColumn.includes("crawler_index_url_id")) {
        parentField = mongoField;
        break;
      }
    }

    const parentValue = parentField ? this.extractFieldValue(doc, parentField) : null;

    // Generate one INSERT per array item
    for (const item of array) {
      const values: Record<string, any> = {};

      for (const [sqlColumn, mongoField] of Object.entries(
        tableConfig.field_mapping
      )) {
        if (mongoField.includes("[]")) {
          // Extract from array item (e.g., "pages[].urlId" -> extract "urlId" from item)
          const fieldName = mongoField.replace(`${arrayPath}[].`, "");
          values[sqlColumn] = item[fieldName];
        } else if (sqlColumn.includes("crawler_index_url_id") && parentValue) {
          // Use parent value
          values[sqlColumn] = parentValue;
        } else {
          // Regular field
          const value = this.extractFieldValue(doc, mongoField);
          if (value !== null && value !== undefined) {
            values[sqlColumn] = value;
          }
        }
      }

      // Skip if no values
      if (Object.keys(values).length === 0) {
        continue;
      }

      // Generate INSERT
      const columns = Object.keys(values).join(", ");
      const placeholders = Object.values(values)
        .map((v, idx) => this.formatValue(v, Object.keys(values)[idx]))
        .join(", ");

      const insertSQL = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders});`;
      inserts.push(insertSQL);
    }

    return inserts;
  }

  /**
   * Extract field value from document using dot notation
   */
  private extractFieldValue(doc: any, fieldPath: string): any {
    const parts = fieldPath.split(".");
    let value: any = doc;

    for (const part of parts) {
      if (value && typeof value === "object" && !Array.isArray(value)) {
        value = value[part];
      } else {
        return null;
      }

      if (value === null || value === undefined) {
        return null;
      }
    }

    return value;
  }

  /**
   * Format value for SQL insertion
   * @param value - The value to format
   * @param columnName - Optional column name to determine conversion type
   */
  private formatValue(value: any, columnName?: string): string {
    if (value === null || value === undefined) {
      return "NULL";
    } else if (typeof value === "boolean") {
      if (this.dbType === "sqlite") {
        return value ? "1" : "0";
      }
      return value ? "TRUE" : "FALSE";
    } else if (typeof value === "number") {
      return String(value);
    } else if (value instanceof Date) {
      // Convert Date to Unix timestamp (seconds) for sqlite/d1
      if (this.dbType === "sqlite" && columnName?.endsWith("_at")) {
        return String(Math.floor(value.getTime() / 1000));
      }
      // For other DB types, use ISO string
      return `'${value.toISOString()}'`;
    } else if (Array.isArray(value)) {
      // Convert array to JSON string (for categories, tags, etc.)
      const jsonStr = JSON.stringify(value).replace(/'/g, "''");
      return `'${jsonStr}'`;
    } else if (typeof value === "object") {
      // Convert object to JSON string
      const jsonStr = JSON.stringify(value).replace(/'/g, "''");
      return `'${jsonStr}'`;
    } else {
      // String - escape single quotes
      const escaped = String(value).replace(/'/g, "''");
      return `'${escaped}'`;
    }
  }
}

/**
 * Create an example schema configuration
 */
function createExampleSchema(): SchemaConfig {
  return {
    tables: {
      users: {
        primary_key: "id",
        columns: {
          id: "id",
          username: "string",
          email: "string",
          created_at: "datetime",
          is_active: "boolean",
          profile_data: "json",
        },
        not_null: ["username", "email"],
        field_mapping: {
          id: "_id",
          username: "username",
          email: "email",
          created_at: "createdAt",
          is_active: "active",
          profile_data: "profile",
        },
      },
    },
  };
}

/**
 * Main execution function
 */
function main() {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.log(
      "Usage: ts-node bson-to-sql.ts <bson_file> <schema_file.json> [database_type]"
    );
    console.log("\nExample schema format:");
    console.log(JSON.stringify(createExampleSchema(), null, 2));
    process.exit(1);
  }

  const bsonFile = args[0];
  const schemaFile = args[1];
  const dbType = (args[2] || "mysql") as DatabaseType;

  // Validate database type
  if (!["postgresql", "mysql", "sqlite"].includes(dbType)) {
    console.error(`Invalid database type: ${dbType}`);
    console.error("Valid options: postgresql, mysql, sqlite");
    process.exit(1);
  }

  // Load schema
  let schema: SchemaConfig;
  try {
    const schemaContent = fs.readFileSync(schemaFile, "utf-8");
    schema = JSON.parse(schemaContent);
  } catch (e) {
    console.error(`Error loading schema file: ${e}`);
    process.exit(1);
  }

  // Convert
  const converter = new BSONToSQLConverter(schema, dbType);

  try {
    const sqlScript = converter.convertBSONFile(bsonFile);
    console.log(sqlScript);
  } catch (e) {
    console.error(`Error converting BSON file: ${e}`);
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main();
}

