/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
public class SchemaRecoveryIT
{
    @Inject
    private volatile EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseAPI db;

    @AfterEach
    void shutdownDatabase()
    {
        if ( db != null )
        {
            db.shutdown();
            db = null;
        }
    }

    @Test
    void schemaTransactionsShouldSurviveRecovery()
    {
        // given
        Label label = label( "User" );
        String property = "email";
        startDb();
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label ).setProperty( property, "neo4j@neo4j.com" );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.success();
        }
        killDb();

        // when
        startDb();

        // then
        assertEquals( 1, constraints( db ).size() );
        assertEquals( 1, indexes( db ).size() );

        db.shutdown();
    }

    private void startDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }

        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs );
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase( testDirectory.databaseDir() );
    }

    private void killDb()
    {
        if ( db != null )
        {
            fs = fs.snapshot();
            db.shutdown();
        }
    }

    private List<ConstraintDefinition> constraints( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            return Iterables.asList( database.schema().getConstraints() );
        }
    }

    private List<IndexDefinition> indexes( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            return Iterables.asList( database.schema().getIndexes() );
        }
    }
}
