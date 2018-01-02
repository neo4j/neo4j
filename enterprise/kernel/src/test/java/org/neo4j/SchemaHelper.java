/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

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
        db.execute( String.format( "CREATE INDEX ON :`%s`(`%s`)", label, property ) );
    }

    public static void createUniquenessConstraint( GraphDatabaseService db, Label label, String property )
    {
        createUniquenessConstraint( db, label.name(), property );
    }

    public static void createUniquenessConstraint( GraphDatabaseService db, String label, String property )
    {
        db.execute( String.format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", label, property ) );
    }

    public static void createNodePropertyExistenceConstraint( GraphDatabaseService db, Label label, String property )
    {
        createNodePropertyExistenceConstraint( db, label.name(), property );
    }

    public static void createNodePropertyExistenceConstraint( GraphDatabaseService db, String label, String property )
    {
        db.execute( String.format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT exists(n.`%s`)", label, property ) );
    }

    public static void createRelPropertyExistenceConstraint( GraphDatabaseService db, RelationshipType type,
            String property )
    {
        createRelPropertyExistenceConstraint( db, type.name(), property );
    }

    public static void createRelPropertyExistenceConstraint( GraphDatabaseService db, String type, String property )
    {
        db.execute( String.format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT exists(r.`%s`)", type, property ) );
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
