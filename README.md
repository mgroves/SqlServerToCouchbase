# SQL Server to Couchbase migration library

A library to help guide your SQL Server to Couchbase migration/sync efforts. This library represents a best effort at mapping SQL Server concepts to Couchbase Server concepts *automatically*. It may or may not fulfill every one of your requirements. In the worst case, it can at least be an educational tool. Think of it like Google Translate.

## Use

Create a `SqlToCb` object with config (and logger of your choice). Execute the `Migrate` method with various flags to limit/expand what you want to try migrating.

`SqlToCbConfig` contains SQL Server connection string, Couchbase connection string/credentials, and other settings.

## Migrations

_Catalog to Bucket_ - For the given *database* (aka "catalog") in SQL Server, create a *bucket* in Couchbase Server (named according to `SqlToCbConfig::TargetBucket`).

_Tables to Collections_ - For each *table* in the given SQL Server database catalog, create a corresponding *collection* in a Couchbase Server bucket. This will also involve the *schema* which you may choose to correspond to a *scope* in Couchbase Server.

Examples:

`Config::UseSchemaForScope = true`

Schema | Table | <-> | Scope | Collection
--- | --- | --- | --- | ---
Person | Address | | Person | Address
Sales | Contact | | Sales | Contact
dbo | Invoice | | _default** | Invoice

`Config::UseSchemaForScope = false`

Schema | Table | <-> | Scope | Collection
--- | --- | --- | --- | ---
Person | Address | | _default | Person_Address
Sales | Contact | | _default | Sales_Contact
dbo | Invoice | | _default** | Invoice

** The rough equivalent of `dbo`  in Couchbase Server is _default.

_Why would I ever want `UserSchemaForScope = false`?_ Scope can be used in Couchbase Server for multi-tenancy. In which case, you'd likely want to reserve scopes to correspond to tenants.

_Indexes to Indexes_ - Indexes designed for relational access may not always be optimal for Couchbase access. However, Indexes will be created that are roughly equivalent.

Example:

SQL Server: `CREATE INDEX [AK_Person_rowguid] ON [Person].[Person] ([rowguid])`

becomes

Couchbase: `CREATE INDEX AK_Person_rowguid ON Person_Person(rowguid)`

Before using these indexes blindly, I strongly recommend trying your query in Couchbase Server Query Workbench to see if there's a better way to write it and/or to see if Couchbase advises creating a different index.

_Rows to documents_ - As with indexes, the translation here is going to be literal. But to truly maximize Couchbase data, you may want to (eventually) consolidate the "translated" documents. (e.g. consolidate Invoice row with PK 123 and corresponding InvoiceItems rows into a single Invoice document with key 123).

Examples:

Starting with a `dbo.Person` Schema/Table in SQL Server:
Id | Name | ShoeSize
--- | --- | ---
55 | Matt D | 14
77 | Emma R | 6

Those rows of data will be created in the `_default.Person` Scope/Collection in Couchbase:
```
key: 55
{ Id: 55, Name: 'Matt D', ShoeSize: 14}

key: 77
{ Id: 77, Name: 'Emma R', ShoeSize: 6}
```

Notice that Id remains intact in the document body. This is to ease the transition, but in the long run it may not be a good idea to keep this "duplicated" data that's already part of the document key.

If the primary key in the SQL Server table is a compound key, the values will be combined into a single document key in Couchbase, separated by "::".

Example:

Key1Id | Key2Id | Name
--- | --- | ---
55 | 14 | Matt D
77 | 6 | Emma R

The documentss for the equivalent data in Couchbase will be:
```
key: 55::14
{ Key1Id: 55, Key2Id: 14, Name: 'Matt D' }

key: 77::6
{ Key1Id: 77, Key2Id: 6, Name: 'Emma R' }
```

Also note that *most* data in SQL Server translates to JSON in a relatively straightforward way. You may want to pay special attention to dates and SQL Server specific types (like geometry). Again, a best effort translation is made. Improvements or suggestions are welcome :)

_Users to users_

SQL Server has multiple kinds of users. This program will look at all users in sysusers that have DB access and create a user in Couchbase by the same name.

It will also give this user *roughly* the same permissions:

SQL Server Permission | Couchbase Permission(s)
--- | ---
SELECT | Query Select, Data Reader
INSERT | Query Insert, Data Writer
UPDATE | Query Update, Data Writer
DELETE | Query Delete, Data Writer

If the user doesn't have any specific permissions, it will give that user Bucket Admin access (which gives the user unlimited access to all features of the bucket).

For complex auth scenarios, users are an area that require a manual audit; user migration is definitely intended as a learning tool only.

## Known limitations:

* SQL Server views are not translated at all. Couchbase M/R Views are *roughly* equivalent.

* SQL Server stored procedures (sprocs) and UDFs are not translated at all. Couchbase UDFs are *roughtly* equivalent.

* SQL Server triggers are not translated at all. Couchbase Eventing is the closest equivalent.

* Collection names in Couchbase are limited to 30 characters. So if a table name (or schema+table name depending on your config) is longer than 30 characters, you'll need to provider a mapping in `Config::TableNameToCollectionMapping`

* HierarchyId data type - this SQL Server specific data type can *possibly* be translated to JSON, but for now it doesn't translate to anything meaningful

* Temporal tables - Currently the "Archive" tables are not being copied over, only the snapshots. Temporal data capabilities are not built into Couchbase, they would need to be added with a combination of client code and/or Couchbase Eventing.
