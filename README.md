# SQL Server to Couchbase migration library

A library to help guide your SQL Server to Couchbase migration/sync efforts. This library represents a best effort at mapping SQL Server concepts to Couchbase Server concepts *automatically*. It may or may not fulfill every one of your requirements. In the worst case, it can at least be an educational tool. Think of it like Google Translate.

## Use

Create a `SqlToCb` object with config (and logger of your choice). Execute the `Migrate` method with various flags to limit/expand what you want to try migrating.

`SqlToCbConfig` contains SQL Server connection string, Couchbase connection string/credentials, and other settings.

## Migrations

_Catalog to Bucket_ - For the given *database* in SQL Server, create a *bucket* in Couchbase Server (named according to `SqlToCbConfig::TargetBucket`).

_Tables to Collections_ - For each *table* in the given SQL Server database catalog, create a corresponding *collection* in a Couchbase Server bucket. This will also involve the *schema* which you may choose to correspond to a *scope* in Couchbase Server.

Examples:

`Config::UseSchemaForScope = true`

Schema | Table | <-> | Scope | Collection
--- | --- | --- | --- | ---
Person | Address | | Person | Address
Sales | Contact | | Sales | Contact
dbo | Invoice | | _default** | Invoice

** The rough equivalent of `dbo`  in Couchbase Server is _default. But if you set `Config::UseSchemaForScope` you can turn this off a scope called `dbo` will be created instead.

`Config::UseSchemaForScope = false`

Schema | Table | <-> | Scope | Collection
--- | --- | --- | --- | ---
Person | Address | | _default | Person_Address
Sales | Contact | | _default | Sales_Contact
dbo | Invoice | | _default** | Invoice

** Again, you can use `Config::UseSchemaForScope` to create a scope called `dbo` if you'd like.

_Why would I ever want `UserSchemaForScope = false`?_ Scope can be used in Couchbase Server for multi-tenancy. In which case, you'd likely want to reserve scopes to correspond to tenants.

_Indexes to Indexes_ - Indexes designed for relational access may not always be optimal for Couchbase access. However, Indexes will be created that are roughly equivalent.

Example:

SQL Server: `CREATE INDEX [AK_Person_rowguid] ON [Person].[Person] ([rowguid])`

becomes

Couchbase: `CREATE INDEX AK_Person_rowguid ON Person_Person(rowguid)`

Before using these indexes blindly, I strongly recommend trying your query in Couchbase Server Query Workbench to see if there's a better way to write it and/or to see if Couchbase advises creating a different index.

_Rows to documents_ - As with indexes, the translation here is going to be literal. But to truly maximize Couchbase data, you may want to (eventually) consolidate the "translated" documents. (e.g. consolidate Invoice row with PK 123 and corresponding InvoiceItems rows into a single Invoice document with key 123).

Also, since integers are often used as primary keys, the resultant key in Couchbase will be pre-pended with the scope+collection name (see above) to guarantee uniqueness.

Examples:

Starting with a `dbo.Person` Schema/Table in SQL Server:
Id | Name | ShoeSize
--- | --- | ---
55 | Matt D | 14
77 | Emma R | 6

Those rows of data will be created in the `_default.Person` Scope/Collection in Couchbase:
```
key: Person::55
{ Id: 55, Name: 'Matt D', ShoeSize: 14}

key: Person::77
{ Id: 77, Name: 'Emma R', ShoeSize: 6}
```

Notice that Id remains intact in the document body. This is to ease the transition, but in the long run it may not be a good idea to keep this "duplicated" data that's already part of the document key.

Also note that *most* data in SQL Server translates to JSON in a relatively straightforward way. You may want to pay special attention to dates and SQL Server specific types (like geometry). Again, a best effort translation is made. Improvements or suggestions are welcome :)

## Known limitations:

* HierarchyId data type - this SQL Server specific data type can *possibly* be translated to JSON, but for now it doesn't translate to anything meaningful

* Collection names in Couchbase are limited to 30 characters. So if a table name (or schema+table name depending on your config) is longer than 30 characters, you'll need to provider a mapping in `Config::TableNameToCollectionMapping`
