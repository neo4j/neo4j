/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

public class IndexSamplingIntegrationTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private final Label label = DynamicLabel.label( "Person" );
    private final String property = "name";
    private final int nodes = 1000;
    private final String[] names = {"Neo4j", "Neo", "Graph", "Apa"};

    @Test
    public void shouldSampleNotUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        long deletedNodes = 0;
        try
        {
            // Given
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir().getAbsolutePath() );
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
                    db.findNodes( label, property, names[i % names.length] ).next().delete();
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

        // sampling will consider also the delete nodes till the next lucene compaction
        DoubleLongRegister register = fetchIndexSamplingValues( db );
        assertEquals( names.length, register.readFirst() );
        assertEquals( nodes, register.readSecond() );

        // but the deleted nodes should not be considered in the index size value
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
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir().getAbsolutePath() );
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

    private DoubleLongRegister fetchIndexSamplingValues( GraphDatabaseService db )
    {
        try
        {
            // Then
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir().getAbsolutePath() );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            CountsTracker countsTracker = api.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
            IndexSampleKey key = CountsKeyFactory.indexSampleKey( 0, 0 ); // cheating a bit...
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

    private DoubleLongRegister fetchIndexSizeValues( GraphDatabaseService db )
    {
        try
        {
            // Then
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.graphDbDir().getAbsolutePath() );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            CountsTracker countsTracker = api.getDependencyResolver().resolveDependency( NeoStores.class ).getCounts();
            IndexStatisticsKey key = CountsKeyFactory.indexStatisticsKey( 0, 0 ); // cheating a bit...
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
