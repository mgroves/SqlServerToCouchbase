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

Couchbase: `CREATE INDEX sql_AK_Person_rowguid ON Person_Person(rowguid)`

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

Notice that Id remains intact in the document body. This is to ease the transition, but in the long run it may not be a good idea to keep this "duplicated" data that's already in the document key.

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

## Filters/Transform Pipelines

By default, _all_ data is copied over _as is_ from SQL Server to Couchbase.

You can add "pipelines" to your migration if you want to filter and/or transform data that's being copied over.

_Filter_: Logic to decide whether or not to include a given piece of data. E.g. "only copy a row of data if 'createDate' is later than '2020-08-18'".

_Transform_: How to change data as its being copied. E.g. "when copying a row of data, change the state value from the 2-letter abbreviation 'OH' to the full state name 'Ohio'"

To create a filter and/or transform, create a class that inherits from SqlPipelineBase. You can override one or more of the methods to add filtering/transforming behavior:

* **Query**: The query that will pull the data from SQL Server. By default, this is `SELECT * FROM [SchemaName].[TableName]`, but you can override it. By using a query with more specificity than `SELECT *`, you can transform the data. By using a query with `WHERE`, you can filter the data.

* **IsIncluded**: When overriding this method for filtering, you can supply logic that returns true to include the data, or false to exclude the data.

* **Transform**: When overriding this method for transforming, you can supply logic that transforms the given piece of data and return whatever you'd like.

Once you've created your class, instatiate it and associate it with a schema+table. See **SamplePipelines.cs** for some examples of pipeline classes. Create a `SqlPipelines` object and add your filters to it. Then give that pipelines object to the `Migrate` method.

```
var pipelines = new SqlPipelines();
pipelines.Add(new ModifiedDateSqlFilter(new DateTime(2014, 05, 27), "Person", "Address"));

// ... snip ...

await convert.MigrateAsync(copyData: true, pipelines: pipelines);
```

You may only add one filter/transform object per SQL Server table.

_Why override `IsIncluded` or `Transform` instead of overriding `Query`? Isn't changing the query going to be more efficient?_ Yes, giving a custom SQL query is going to be the most efficient way to filter/transform. However, if you have complex logic better expressed in C# and/or have need to call out to services that SQL Server doesn't have access to, you can use these functions instead.

## Denormalization

After the data is copied over, you can run one or more denormalizations. The `SqlToCbConfig` object you use has a `DenormalizeMaps` property. Denormalization definitions are applied **in order** to the documents. So if you want to denormalize at multiple levels, make sure that the farthest branches of denormalization run first. The documents will be then cascaded up to the root.

Two denormalization objects that are built-in are `ManyToOneDenormalizer` and `OneToOneDenormalizer`. (You can define your own normalizer by implementing `IDenormalizer`).

### ManyToOneDenormalizer

A many-to-one denormalizer means that you will be embedding documents in one collection into their corresponding "parent" documents, using their foreign key values. (Couchbase does not support foreign key constraints, but all the values needed are present in the converted documents).

For example, if you have a ShoppingCart collection and a ShoppingCartItems collection, you can use ManyToOneDenormalizer to embed all the ShoppingCartItems documents into their "parent" ShoppingCart documents.

This object contains two properties: `From` and `To`.

`From` defines the "child" data: SchemaName, TableName, and ForeignKeyNames.

`To` defined the "parent" data: SchemaName and TableName.

After embeddeding, the embedded values will live in an array with a pluralized version of the table name.

For instance, a shopping cart example:

```
From.SchemaName = "Shopping"
From.TableName = "ShoppingCartItems"
From.PrimaryKeyNames = [ "ShoppingCartId" ]

To.SchemaName = "Shopping"
To.TableName = "ShoppingCart"
```

The end result will be documents in the ShoppingCart collection like:

```
{
    "ShoppingCartId" : 123,
    "CartCreatedDt" : "2021-07-15",
    "ShoppingCartItems" : [
        { "ShoppingCartId" : 123, "Desc" : "Widget", "Qty" : 1, "Price" : 9.99 },
        { "ShoppingCartId" : 123, "Desc" : "Doohickey", "Qty" : 3, "Price" : 0.99 },
    ]
}
```

Notice the vestigial `ShoppingCartId` field in the array of nested objects.

The documents in the ShoppingCartItems collection ALSO remain as vestigial data, though it can be easily removed with the Couchbase SDK directly if you want.

### OneToOneDenormalizer

A one-to-one denormalizer means you will be embedding a document from one collection into a document in another collection by using the foreign key value from one document. (Couchbase does not support foreign key constraints, but all the values needed are present in the converted documents).

For example, if you have a document in a Person collection with a `PhoneNumberId` field which corresponds to a document in the PhoneNumber document, the PhoneNumber document will be embedded into the "parent" document.

This object contains two properties: `From` and `To`.

`From` defines the "child" data: SchemaName and TableName.

`To` defined the "parent" data, and some optional transforms: SchemaName, TableName, RemoveForeignKey, Unnest, UnnestSeparator, ForeignKeyNames

For instance, the phone number example:

```
From.SchemaName = "Person"
From.TableName = "PhoneNumber"

To.SchemaName = "Person"
To.TableName = "Person"
To.ForeignKeyNames = [ "HomePhoneNumberId" ]
To.RemoveForeignKey = false
To.Unnest = false
To.UnnestSeparator = "_"
```

The end result will be documents in the Person collection like:

```
{
    "PersonId" : 123,
    "Name" : "Matt",
    "PhoneNumberId" : 456,
    "PhoneNumber" : {
        "PhoneNumberId" : 456,
        "Number" : "111-222-3333"
    }
}
```

Notice the vestigial PhoneNumberId field: if you set RemoveForeignKey to true, this will be automatically removed.

Notice that PhoneNumber is a nested object: if you set Unnest to true, all the fields will be nested up one level. The field names will be prepended with the old table name and whatever unnest seperator you define.

## Known limitations:

* SQL Server views are not translated at all. [Couchbase M/R Views](https://docs.couchbase.com/server/current/learn/views/views-intro.html) are *roughly* equivalent.

* SQL Server stored procedures (sprocs) and UDFs are not translated at all. [Couchbase UDFs](https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/userfun.html) are *roughly* equivalent.

* SQL Server triggers are not translated at all. [Couchbase Eventing](https://docs.couchbase.com/server/current/eventing/eventing-overview.html) is the closest equivalent.

* Collection names in Couchbase are limited to 30 characters. So if a table name (or schema+table name depending on your config) is longer than 30 characters, you'll need to provider a mapping in `Config::TableNameToCollectionMapping`

* HierarchyId data type - this SQL Server specific data type can *possibly* be translated to JSON, but for now it doesn't translate to anything meaningful

* Temporal tables - Currently the "Archive" tables are not being copied over, only the snapshots. Temporal data capabilities are not built into Couchbase, they would need to be added with a combination of client code and/or Couchbase Eventing.

* Denormalization: if you run denormalization on the data multiple times, it will likely result in duplicates. You should rerun the data copy BEFORE denormalization every time.
