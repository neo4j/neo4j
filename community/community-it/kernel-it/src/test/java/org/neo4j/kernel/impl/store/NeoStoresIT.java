/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DbmsExtension( configurationCallback = "configure" )
class NeoStoresIT
{
    @Inject
    private GraphDatabaseAPI db;

    private static final RelationshipType FRIEND = RelationshipType.withName( "FRIEND" );
    private static final String LONG_STRING_VALUE = randomAscii( 2048 );

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.dense_node_threshold, 1 );
    }

    @Test
    void tracePageCacheAccessOnHighIdScan()
    {
        var storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();

        for ( int i = 0; i < 1000; i++ )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                var node = transaction.createNode();
                node.setProperty( "a", randomAscii( 1024 ) );
                transaction.commit();
            }
        }

        var cursorTracer = new DefaultPageCacheTracer().createPageCursorTracer( "tracePageCacheAccessOnHighIdScan" );
        propertyStore.scanForHighId( cursorTracer );

        assertEquals( 6, cursorTracer.hits() );
        assertEquals( 6, cursorTracer.pins() );
        assertEquals( 6, cursorTracer.unpins() );
    }

    @Test
    void tracePageCacheAccessOnGetRawRecordData() throws IOException
    {
        var storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();

        try ( Transaction transaction = db.beginTx() )
        {
            var node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }

        var cursorTracer = new DefaultPageCacheTracer().createPageCursorTracer( "tracePageCacheAccessOnGetRawRecordData" );
        propertyStore.getRawRecordData( 1L, cursorTracer );

        assertEquals( 1, cursorTracer.hits() );
        assertEquals( 1, cursorTracer.pins() );
        assertEquals( 1, cursorTracer.unpins() );
    }

    @Test
    void tracePageCacheAccessOnInUseCheck()
    {
        var storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyStore = neoStores.getPropertyStore();

        try ( Transaction transaction = db.beginTx() )
        {
            var node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }

        var cursorTracer = new DefaultPageCacheTracer().createPageCursorTracer( "tracePageCacheAccessOnInUseCheck" );
        propertyStore.isInUse( 1L, cursorTracer );

        assertEquals( 1, cursorTracer.hits() );
        assertEquals( 1, cursorTracer.pins() );
        assertEquals( 1, cursorTracer.unpins() );
    }

    @Test
    void tracePageCacheAccessOnGetRecord()
    {
        var storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        var neoStores = storageEngine.testAccessNeoStores();
        var nodeStore = neoStores.getNodeStore();

        long nodeId;
        try ( Transaction transaction = db.beginTx() )
        {
            var node = transaction.createNode();
            node.setProperty( "a", "b" );
            nodeId = node.getId();
            transaction.commit();
        }

        var cursorTracer = new DefaultPageCacheTracer().createPageCursorTracer( "tracePageCacheAccessOnGetRecord" );
        nodeStore.getRecord( nodeId, new NodeRecord( nodeId ), RecordLoad.NORMAL, cursorTracer );

        assertEquals( 1, cursorTracer.hits() );
        assertEquals( 1, cursorTracer.pins() );
        assertEquals( 1, cursorTracer.unpins() );
    }

    @Test
    void tracePageCacheAccessOnUpdateRecord()
    {
        var storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        var neoStores = storageEngine.testAccessNeoStores();
        var nodeStore = neoStores.getNodeStore();

        long nodeId;
        try ( Transaction transaction = db.beginTx() )
        {
            var node = transaction.createNode();
            node.setProperty( "a", "b" );
            nodeId = node.getId();
            transaction.commit();
        }

        var cursorTracer = new DefaultPageCacheTracer().createPageCursorTracer( "tracePageCacheAccessOnUpdateRecord" );
        nodeStore.updateRecord( new NodeRecord( nodeId ), cursorTracer );

        assertEquals( 5, cursorTracer.hits() );
        assertEquals( 6, cursorTracer.pins() );
        assertEquals( 6, cursorTracer.unpins() );
    }

    @Test
    void tracePageCacheAccessOnTokenReads()
    {
        var storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        var neoStores = storageEngine.testAccessNeoStores();
        var propertyKeys = neoStores.getPropertyKeyTokenStore();

        try ( Transaction transaction = db.beginTx() )
        {
            var node = transaction.createNode();
            node.setProperty( "a", "b" );
            transaction.commit();
        }

        var cursorTracer = new DefaultPageCacheTracer().createPageCursorTracer( "tracePageCacheAccessOnTokenReads" );
        propertyKeys.getAllReadableTokens( cursorTracer );

        assertEquals( 2, cursorTracer.hits() );
        assertEquals( 2, cursorTracer.pins() );
        assertEquals( 2, cursorTracer.unpins() );
    }

    @Test
    void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord()
            throws Throwable
    {
        Race race = new Race();
        long[] latestNodeId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 2 );
        race.withEndCondition( () -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode();
                latestNodeId[0] = node.getId();
                node.setProperty( "largeProperty", LONG_STRING_VALUE );
                tx.commit();
            }
            writes.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.getNodeById( latestNodeId[0] );
                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.commit();
            }
            catch ( NotFoundException e )
            {
                // This will catch nodes not found (expected) and also PropertyRecords not found (shouldn't happen
                // but handled in shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord)
            }
            reads.incrementAndGet();
        } );
        race.go();
    }

    @Test
    void shouldWriteOutThePropertyRecordBeforeReferencingItFromANodeRecord()
            throws Throwable
    {
        Race race = new Race();
        long[] latestNodeId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 2 );
        race.withEndCondition( () -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode();
                latestNodeId[0] = node.getId();
                node.setProperty( "largeProperty", LONG_STRING_VALUE );
                tx.commit();
            }
            writes.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.getNodeById( latestNodeId[0] );

                for ( String propertyKey : node.getPropertyKeys() )
                {
                    node.getProperty( propertyKey );
                }
                tx.commit();
            }
            catch ( NotFoundException e )
            {
                if ( indexOfThrowable( e, InvalidRecordException.class ) != -1 )
                {
                    throw e;
                }
            }
            reads.incrementAndGet();
        } );
        race.go();
    }

    @Test
    void shouldWriteOutThePropertyRecordBeforeReferencingItFromARelationshipRecord()
            throws Throwable
    {
        final long node1Id;
        final long node2Id;
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode();
            node1Id = node1.getId();

            Node node2 = tx.createNode();
            node2Id = node2.getId();

            tx.commit();
        }

        Race race = new Race();
        final long[] latestRelationshipId = new long[1];
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        long endTime = currentTimeMillis() + SECONDS.toMillis( 2 );
        race.withEndCondition( () -> (writes.get() > 100 && reads.get() > 10_000) || currentTimeMillis() > endTime );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = tx.getNodeById( node1Id );
                Node node2 = tx.getNodeById( node2Id );

                Relationship rel = node1.createRelationshipTo( node2, FRIEND );
                latestRelationshipId[0] = rel.getId();
                rel.setProperty( "largeProperty", LONG_STRING_VALUE );

                tx.commit();
            }
            writes.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Relationship rel = tx.getRelationshipById( latestRelationshipId[0] );

                for ( String propertyKey : rel.getPropertyKeys() )
                {
                    rel.getProperty( propertyKey );
                }
                tx.commit();
            }
            catch ( NotFoundException e )
            {
                if ( indexOfThrowable( e, InvalidRecordException.class ) != -1 )
                {
                    throw e;
                }
            }
            reads.incrementAndGet();
        } );
        race.go();
    }
}
