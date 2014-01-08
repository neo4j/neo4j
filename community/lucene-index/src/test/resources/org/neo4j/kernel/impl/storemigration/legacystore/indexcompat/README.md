This database was created with 2.0.0. We verify that it can still be read by later versions since we have made subtle
changes to how we store values in Lucene.

Cypher for recreating this database:

    CREATE INDEX ON :Person(age)
    CREATE CONSTRAINT ON (person:Person) ASSERT person.externalId IS UNIQUE

    CREATE (:Person { age: 1, externalId: 0 })
    CREATE (:Person { age: 3, externalId: 7 })
    CREATE (:Person { age: 2, externalId: 1 })
    CREATE (:Person { age: 4, externalId: 3 })
    CREATE (:Person { age: 2, externalId: 4 })
    CREATE (:Person { age: 4, externalId: 6 })
    CREATE (:Person { age: 3, externalId: 2 })
    CREATE (:Person { age: 4, externalId: 8 })
    CREATE (:Person { age: 3, externalId: 5 })
    CREATE (:Person { age: 4, externalId: 9 })

Note that the nodes are inserted out of order with respect to all indexed properties, just in case ordered insert would
conceal a subtle incompatibility.
