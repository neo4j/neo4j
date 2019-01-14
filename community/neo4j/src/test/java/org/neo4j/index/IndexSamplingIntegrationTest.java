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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class IndexSamplingIntegrationTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private final Label label = Label.label( "Person" );
    private final String property = "name";
    private final long nodes = 1000;
    private final String[] names = {"Neo4j", "Neo", "Graph", "Apa"};

    @Test
    public void shouldSampleNotUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        long deletedNodes = 0;
        try
        {
            // Given
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
            IndexDefinition indexDefinition;
            try ( Transaction tx = db.beginTx() )
            {
                indexDefinition = db.schema().indexFor( label ).on( property ).create();
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    db.createNode( label ).setProperty( property, names[i % names.length] );
                    tx.success();
                }

            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < (nodes / 10) ; i++ )
                {
                    try ( ResourceIterator<Node> nodes = db.findNodes( label, property, names[i % names.length] ) )
                    {
                        nodes.next().delete();
                    }
                    deletedNodes++;
                    tx.success();
                }
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }

        // When
        triggerIndexResamplingOnNextStartup();

        // Then

        // lucene will consider also the delete nodes, native won't
        DoubleLongRegister register = fetchIndexSamplingValues( db );
        assertEquals( names.length, register.readFirst() );
        assertThat( register.readSecond(), allOf( greaterThanOrEqualTo( nodes - deletedNodes ), lessThanOrEqualTo( nodes ) ) );

        // but regardless, the deleted nodes should not be considered in the index size value
        DoubleLongRegister indexSizeRegister = fetchIndexSizeValues( db );
        assertEquals( 0, indexSizeRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSizeRegister.readSecond() );
    }

    @Test
    public void shouldSampleUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        long deletedNodes = 0;
        try
        {
            // Given
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    db.createNode( label ).setProperty( property, "" + i );
                    tx.success();
                }
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    if ( i % 10 == 0 )
                    {
                        deletedNodes++;
                        db.findNode( label, property, "" + i ).delete();
                        tx.success();
                    }
                }
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }

        // When
        triggerIndexResamplingOnNextStartup();

        // Then
        DoubleLongRegister indexSampleRegister = fetchIndexSamplingValues( db );
        assertEquals( nodes - deletedNodes, indexSampleRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSampleRegister.readSecond() );

        DoubleLongRegister indexSizeRegister = fetchIndexSizeValues( db );
        assertEquals( 0, indexSizeRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSizeRegister.readSecond() );
    }

    private long indexId( GraphDatabaseAPI api ) throws IndexNotFoundKernelException
    {
        ThreadToStatementContextBridge contextBridge =
                api.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        try ( Transaction tx = api.beginTx() )
        {
            KernelTransaction ktx =
                    contextBridge.getKernelTransactionBoundToThisThread( true );
            try ( Statement ignore = ktx.acquireStatement() )
            {
                IndexingService indexingService =
                        api.getDependencyResolver().resolveDependency( IndexingService.class );
                TokenRead tokenRead = ktx.tokenRead();
                int labelId = tokenRead.nodeLabel( label.name() );
                int propertyKeyId = tokenRead.propertyKey( property );
                long indexId = indexingService.getIndexId( SchemaDescriptorFactory.forLabel( labelId, propertyKeyId ) );
                tx.success();
                return indexId;
            }
        }
    }

    private DoubleLongRegister fetchIndexSamplingValues( GraphDatabaseService db ) throws IndexNotFoundKernelException
    {
        try
        {
            // Then
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            CountsTracker countsTracker = api.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                    .testAccessNeoStores().getCounts();
            IndexSampleKey key = CountsKeyFactory.indexSampleKey( indexId( api ) );
            return countsTracker.get( key, Registers.newDoubleLongRegister() );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private DoubleLongRegister fetchIndexSizeValues( GraphDatabaseService db ) throws IndexNotFoundKernelException
    {
        try
        {
            // Then
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir() );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            CountsTracker countsTracker = api.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                    .testAccessNeoStores().getCounts();
            IndexStatisticsKey key = CountsKeyFactory.indexStatisticsKey( indexId( api ) );
            return countsTracker.get( key, Registers.newDoubleLongRegister() );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private void triggerIndexResamplingOnNextStartup()
    {
        // Trigger index resampling on next at startup
        String baseName = MetaDataStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE;
        FileUtils.deleteFile( new File( testDirectory.graphDbDir(), baseName + CountsTracker.LEFT ) );
        FileUtils.deleteFile( new File( testDirectory.graphDbDir(), baseName + CountsTracker.RIGHT ) );
    }
}
