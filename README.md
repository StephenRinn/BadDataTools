# BadDataTools

BadDataTools is a personal tool to create sql queries to hunt down `NULL` fields in a database. This is a quick internal tool created for my design style for slick tables in Scala.

Only real use is when the database is not fully in control of a single team, such as when salesforce and the backend services can alter tables or insert data.

## Features

- Automatic Query Generation – Finds all non-optional fields 
- Full Schema Query Generation - Can be used for one table or for all tables
- Output as a file - Automatically output to a file which can be used to then run query from file

## Arguments

| Argument      | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `--inputDir`  | Directory containing one or more Slick table definition files              |
| `--outputDir` | Destination directory for the generated SQL files *(Default: `/Users/Shared/`)* |
| `--absolutePath` | Absolute path to a single Scala file with a Slick table definition       |

## Example

```
scala BadDataTools --inputDir ./foo --outputDir ./foo/bar --absolutePath /abs/foobar.scala