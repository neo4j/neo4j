/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class SchemaHelper
{
    private SchemaHelper()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static void createIndex( GraphDatabaseService db, Label label, String property )
    {
        createIndex( db, label.name(), property );
    }

    public static void createIndex( GraphDatabaseService db, String label, String property )
    {
        db.execute( format( "CREATE INDEX ON :`%s`(`%s`)", label, property ) );
    }

    public static void createUniquenessConstraint( GraphDatabaseService db, Label label, String property )
    {
        createUniquenessConstraint( db, label.name(), property );
    }

    public static void createUniquenessConstraint( GraphDatabaseService db, String label, String property )
    {
        db.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, property ) );
    }

    public static void createNodeKeyConstraint( GraphDatabaseService db, Label label, String... properties )
    {
        createNodeKeyConstraint( db, label.name(), properties );
    }

    public static void createNodeKeyConstraint( GraphDatabaseService db, String label, String... properties )
    {
        String keyProperties = Arrays.stream( properties )
                .map( property -> format("n.`%s`", property))
                .collect( joining( "," ) );
        db.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT (%s) IS NODE KEY", label, keyProperties ) );
    }

    public static void createNodePropertyExistenceConstraint( GraphDatabaseService db, Label label, String property )
    {
        createNodePropertyExistenceConstraint( db, label.name(), property );
    }

    public static void createNodePropertyExistenceConstraint( GraphDatabaseService db, String label, String property )
    {
        db.execute( format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT exists(n.`%s`)", label, property ) );
    }

    public static void createRelPropertyExistenceConstraint( GraphDatabaseService db, RelationshipType type,
            String property )
    {
        createRelPropertyExistenceConstraint( db, type.name(), property );
    }

    public static void createRelPropertyExistenceConstraint( GraphDatabaseService db, String type, String property )
    {
        db.execute( format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT exists(r.`%s`)", type, property ) );
    }

    public static void awaitIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }
}
