# Data Vault 2.0 Modeling with Apache Spark

Data vault modeling is a database modeling method that is designed to provide long-term historical storage of data coming in from multiple operational systems. It is also a method of looking at historical data that deals with issues such as auditing, tracing of data, loading speed and resilience to change as well as emphasizing the need to trace where all the data in the database came from. This means that every row in a data vault must be accompanied by record source and load date attributes, enabling an auditor to trace values back to the source.

#### Some facts: 
* It’s a component of an enterprise data warehouse (EDW)
* Like third-normal form (3NF) or star/snowflake schemas, it’s a way to model your data
* It’s really well-suited to auditing and providing historical snapshots, because: 
  * when designed properly, it contains all of the source data, just transformed
  * its structure adapts elegantly as your source data models change, without loss of data
  * rows are immutable (you only do inserts, never updates or deletes)
* It is not intended to be queried directly, but its tables can be extended or transformed to another format for this purpose

#### Key Concepts: Hubs, Links, and Satellites

Hubs, Links, and Satellites are the three primary table types used in Data Vault 2.0, each with a clear purpose, and rules governing their structure.

_**Hubs:**_
Hub tables contain unique “business keys.” A business key is any value that has meaning for the business (so, a natural key, not a surrogate key).

_**Links:**_
Link tables are pretty simple. They link two (or more) Hubs together, because they represent the relationship between two (or more) business keys. They always act as the join table in a many-to-many relationship, for flexibility.

_**Satellites:**_
Satellite tables are the closest thing to a traditional data warehouse table in DV2, and are the most important part of a DV2 model. They capture ALL changes to descriptive data over time, and in that regard, they are very similar to Type 2 slowly-changing dimensions. Satellite tables contain all of the columns from the source table (or tables) we haven’t yet put somewhere else, and they can be associated with a Hub or a Link table.


## Description of this project 

This project is an example of how to use Apache Spark for modeling a Datawarehouse with Data Vault 2.0.

I wrote a Scala Object that automatically creates HUB and SAT tables for a model, given a number of parameters. 
Also, the creation of Links between HUB's it's automated.


## Tech
* Scala 2.11
* SBT 1.3
* Apache Spark 2.2.1

        

### TODO

 - Write Tests
 - Finish the Enterprise Stage

### References

1. https://en.wikipedia.org/wiki/Data_vault_modeling
2. https://blog.fairwaytech.com/data-vault-2.0-modeling-a-real-world-example
