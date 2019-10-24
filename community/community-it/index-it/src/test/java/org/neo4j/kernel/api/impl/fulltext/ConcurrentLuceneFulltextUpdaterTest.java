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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.Race;
import org.neo4j.test.rule.RepeatRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;

/**
 * Concurrent updates and index changes should result in valid state, and not create conflicts or exceptions during
 * commit.
 */
public class ConcurrentLuceneFulltextUpdaterTest extends LuceneFulltextTestSupport
{
    private final int aliceThreads = 1;
    private final int bobThreads = 1;
    private final int nodesCreatedPerThread = 500;
    private Race race;
    private CountDownLatch aliceLatch = new CountDownLatch( 2 );
    private CountDownLatch bobLatch = new CountDownLatch( 2 );

    @Override
    protected RepeatRule createRepeatRule()
    {
        return new RepeatRule( false, 1 );
    }

    @Before
    public void createRace()
    {
        race = new Race();
    }

    private void createInitialIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( FULLTEXT ).withName( "nodes" ).create();
            tx.commit();
        }
    }

    private void raceContestantsAndVerifyResults( Runnable aliceWork, Runnable changeConfig, Runnable bobWork ) throws Throwable
    {
        race.addContestants( aliceThreads, aliceWork );
        race.addContestant( changeConfig );
        race.addContestants( bobThreads, bobWork );
        race.go();
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "nodes", 30, TimeUnit.SECONDS );
        }
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            IndexReadSession index = ktx.dataRead().indexReadSession( ktx.schemaRead().indexGetForName( "nodes" ) );
            try ( NodeValueIndexCursor bobCursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( index, bobCursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( "bob" ) );
                int bobCount = 0;
                while ( bobCursor.next() )
                {
                    bobCount += 1;
                }
                assertEquals( bobThreads * nodesCreatedPerThread, bobCount );
            }
            try ( NodeValueIndexCursor aliceCursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( index, aliceCursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( "alice" ) );
                int aliceCount = 0;
                while ( aliceCursor.next() )
                {
                    aliceCount += 1;
                }
                assertEquals( 0, aliceCount );
            }
        }
    }

    private Runnable work( int iterations, ThrowingConsumer<Transaction, Exception> work )
    {
        return () ->
        {
            try
            {
                for ( int i = 0; i < iterations; i++ )
                {
                    Thread.yield();
                    try ( Transaction tx = db.beginTx() )
                    {
                        Thread.yield();
                        work.accept( tx );
                        Thread.yield();
                        tx.commit();
                    }
                }
            }
            catch ( Exception e )
            {
                throw new AssertionError( e );
            }
        };
    }

    private ThrowingAction<Exception> dropAndReCreateIndex()
    {
        return () ->
        {
            aliceLatch.await();
            bobLatch.await();
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().getIndexByName( "nodes" ).drop();
                tx.schema().indexFor( LABEL ).on( "otherProp" ).withIndexType( FULLTEXT ).withName( "nodes" ).create();
                tx.commit();
            }
        };
    }

    @Test
    public void labelledNodesCoreAPI() throws Throwable
    {
        createInitialIndex();

        Runnable aliceWork = work( nodesCreatedPerThread, tx ->
        {
            tx.getNodeById( createNodeIndexableByPropertyValue( tx, LABEL, "alice" ) );
            aliceLatch.countDown();
        } );
        Runnable bobWork = work( nodesCreatedPerThread, tx ->
        {
            tx.getNodeById( createNodeWithProperty( tx, LABEL, "otherProp", "bob" ) );
            bobLatch.countDown();
        } );
        Runnable changeConfig = work( 1, tx -> dropAndReCreateIndex().apply() );
        raceContestantsAndVerifyResults( aliceWork, changeConfig, bobWork );
    }

    @Test
    public void labelledNodesCypherCurrent() throws Throwable
    {
        createInitialIndex();

        Runnable aliceWork = work( nodesCreatedPerThread, tx ->
        {
            tx.execute( "create (:LABEL {" + PROP + ": \"alice\"})" ).close();
            aliceLatch.countDown();
        } );
        Runnable bobWork = work( nodesCreatedPerThread, tx ->
        {
            tx.execute( "create (:LABEL {otherProp: \"bob\"})" ).close();
            bobLatch.countDown();
        } );
        Runnable changeConfig = work( 1, tx -> dropAndReCreateIndex().apply() );
        raceContestantsAndVerifyResults( aliceWork, changeConfig, bobWork );
    }
}
