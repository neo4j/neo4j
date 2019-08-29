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
package org.neo4j.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.snapshot.TestTransactionVersionContextSupplier;
import org.neo4j.snapshot.TestVersionContext;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class QueryRestartIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private TestTransactionVersionContextSupplier testContextSupplier;
    private File storeDir;
    private TestVersionContext testCursorContext;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        storeDir = testDirectory.directory();
        testContextSupplier = new TestTransactionVersionContextSupplier();
        database = startSnapshotQueryDb();
        createData();

        testCursorContext = TestVersionContext.testCursorContext( managementService, DEFAULT_DATABASE_NAME );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void executeQueryWithoutRestarts()
    {
        testCursorContext.setWrongLastClosedTxId( false );

        try ( Transaction transaction = database.beginTx() )
        {
            Result result = database.execute( "MATCH (n:label) RETURN n.c" );
            while ( result.hasNext() )
            {
                assertEquals( "d", result.next().get( "n.c" ) );
            }
            assertEquals( 0, testCursorContext.getAdditionalAttempts() );
            transaction.commit();
        }
    }

    @Test
    void executeQueryWithSingleRetry()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result result = database.execute( "MATCH (n) RETURN n.c" );
            assertEquals( 1, testCursorContext.getAdditionalAttempts() );
            while ( result.hasNext() )
            {
                assertEquals( "d", result.next().get( "n.c" ) );
            }
            transaction.commit();
        }
    }

    @Test
    void executeCountStoreQueryWithSingleRetry()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result result = database.execute( "MATCH (n:toRetry) RETURN count(n)" );
            assertEquals( 1, testCursorContext.getAdditionalAttempts() );
            while ( result.hasNext() )
            {
                assertEquals( 1L, result.next().get( "count(n)" ) );
            }
            transaction.commit();
        }
    }

    @Test
    void executeLabelScanQueryWithSingleRetry()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Result result = database.execute( "MATCH (n:toRetry) RETURN n.c" );
            assertEquals( 1, testCursorContext.getAdditionalAttempts() );
            while ( result.hasNext() )
            {
                assertEquals( "d", result.next().get( "n.c" ) );
            }
            transaction.commit();
        }
    }

    @Test
    void queryThatModifiesDataAndSeesUnstableSnapshotShouldThrowException()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            QueryExecutionException e = assertThrows( QueryExecutionException.class, () -> database.execute( "MATCH (n:toRetry) CREATE () RETURN n.c" ) );
            assertEquals( "Unable to get clean data snapshot for query " + "'MATCH (n:toRetry) CREATE () RETURN n.c' that performs updates.", e.getMessage() );
            transaction.commit();
        }
    }

    private GraphDatabaseService startSnapshotQueryDb()
    {
        // Inject TransactionVersionContextSupplier
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( testContextSupplier );

        managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setExternalDependencies( dependencies )
                .setConfig( GraphDatabaseSettings.snapshot_query, true )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void createData()
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "d" );
            transaction.commit();
        }
    }
}
