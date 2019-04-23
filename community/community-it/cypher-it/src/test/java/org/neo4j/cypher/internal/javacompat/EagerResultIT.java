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
package org.neo4j.cypher.internal.javacompat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongSupplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.impl.context.TransactionVersionContext;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EagerResultIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private GraphDatabaseService database;
    private TestTransactionVersionContextSupplier testContextSupplier;
    private File storeDir;
    private TestVersionContext testCursorContext;
    private DatabaseManagementService managementService;

    @Before
    public void setUp()
    {
        storeDir = testDirectory.directory();
        testContextSupplier = new TestTransactionVersionContextSupplier();
        database = startRestartableDatabase();
        prepareData();
        TransactionIdStore transactionIdStore = getTransactionIdStore();
        testCursorContext = new TestVersionContext( transactionIdStore::getLastClosedTransactionId );
        testContextSupplier.setCursorContext( testCursorContext );
    }

    @After
    public void tearDown()
    {
        if ( database != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    public void eagerResultContainsAllData()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        int rows = 0;
        while ( result.hasNext() )
        {
            result.next();
            rows++;
        }
        assertEquals( 2, rows );
    }

    @Test
    public void eagerResultContainsExecutionType()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        assertEquals( QueryExecutionType.query( QueryExecutionType.QueryType.READ_ONLY ), result.getQueryExecutionType() );
    }

    @Test
    public void eagerResultContainsColumns()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c as a, count(n) as b" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        assertEquals( Arrays.asList("a", "b"), result.columns() );
    }

    @Test
    public void useColumnAsOnEagerResult()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c as c, n.b as b" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        ResourceIterator<Object> cValues = result.columnAs( "c" );
        int rows = 0;
        while ( cValues.hasNext() )
        {
            cValues.next();
            rows++;
        }
        assertEquals( 2, rows );
    }

    @Test
    public void eagerResultHaveQueryStatistic()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        assertFalse( result.getQueryStatistics().containsUpdates() );
    }

    @Test
    public void eagerResultHaveExecutionPlan()
    {
        Result result = database.execute( "profile MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        assertEquals( 2, result.getExecutionPlanDescription().getProfilerStatistics().getRows() );
    }

    @Test
    public void eagerResultToString()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c, n.d" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        String resultString = result.resultAsString();
        assertTrue( resultString.contains( "n.c, n.d" ) );
        assertTrue( resultString.contains( "d, a" ) );
        assertTrue( resultString.contains( "y, k" ) );
    }

    @Test
    public void eagerResultWriteAsStringToStream()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        assertEquals( result.resultAsString(), printToStream( result ) );
    }

    @Test
    public void eagerResultVisit() throws Exception
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        List<String> values = new ArrayList<>();
        result.accept( (Result.ResultVisitor<Exception>) row ->
        {
            values.add( row.getString( "n.c" ) );
            return false;
        } );
        assertThat( values, hasSize( 2 ) );
        assertThat( values, containsInAnyOrder( "d", "y" ) );
    }

    @Test( expected = QueryExecutionException.class )
    public void dirtyContextDuringResultVisitResultInUnstableSnapshotException() throws Exception
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        List<String> values = new ArrayList<>();
        result.accept( (Result.ResultVisitor<Exception>) row ->
        {
            testCursorContext.markAsDirty();
            values.add( row.getString( "n.c" ) );
            return false;
        } );
    }

    @Test( expected = QueryExecutionException.class )
    public void dirtyContextEntityNotFoundExceptionDuringResultVisitResultInUnstableSnapshotException() throws Exception
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        result.accept( (Result.ResultVisitor<Exception>) row ->
        {
            testCursorContext.markAsDirty();
            throw new NotFoundException( new RuntimeException() );
        } );
    }

    private String printToStream( Result result )
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter( stringWriter );
        result.writeAsStringTo( printWriter );
        printWriter.flush();
        return stringWriter.toString();
    }

    private void prepareData()
    {
        Label label = Label.label( "label" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "d" );
            node.setProperty( "d", "a" );
            transaction.success();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "y" );
            node.setProperty( "d", "k" );
            transaction.success();
        }
    }

    private GraphDatabaseService startRestartableDatabase()
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( testContextSupplier );
        managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setExternalDependencies( dependencies )
                .setConfig( GraphDatabaseSettings.snapshot_query, Settings.TRUE ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private TransactionIdStore getTransactionIdStore()
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return dependencyResolver.resolveDependency( TransactionIdStore.class );
    }

    private class TestVersionContext extends TransactionVersionContext
    {

        private boolean useCorrectLastCommittedTxId;
        private int additionalAttempts;

        TestVersionContext( LongSupplier transactionIdSupplier )
        {
            super( transactionIdSupplier );
        }

        @Override
        public long lastClosedTransactionId()
        {
            return useCorrectLastCommittedTxId ? TransactionIdStore.BASE_TX_ID : super.lastClosedTransactionId();
        }

        @Override
        public void markAsDirty()
        {
            super.markAsDirty();
            useCorrectLastCommittedTxId = true;
        }

        @Override
        public boolean isDirty()
        {
            additionalAttempts++;
            return super.isDirty();
        }

        int getAdditionalAttempts()
        {
            return additionalAttempts;
        }
    }

    private class TestTransactionVersionContextSupplier extends TransactionVersionContextSupplier
    {
        void setCursorContext( VersionContext versionContext )
        {
            this.cursorContext.set( versionContext );
        }
    }
}
