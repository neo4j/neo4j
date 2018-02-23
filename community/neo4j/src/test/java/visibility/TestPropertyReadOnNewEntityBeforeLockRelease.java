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
package visibility;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith( ImpermanentDatabaseExtension.class )
public class TestPropertyReadOnNewEntityBeforeLockRelease
{
    private static final String INDEX_NAME = "nodes";
    private static final int MAX_READER_DELAY_MS = 10;

    @Resource
    public ImpermanentDatabaseRule db;

    @BeforeEach
    public void initializeIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            db.index().forNodes( INDEX_NAME ).add( node, "foo", "bar" );
            tx.success();
        }
    }

    @RepeatedTest( 100 )
    public void shouldBeAbleToReadPropertiesFromNewNodeReturnedFromIndex() throws Exception
    {
        String propertyKey = UUID.randomUUID().toString();
        String propertyValue = UUID.randomUUID().toString();
        AtomicBoolean start = new AtomicBoolean( false );
        int readerDelay = ThreadLocalRandom.current().nextInt( MAX_READER_DELAY_MS );

        Writer writer = new Writer( db, propertyKey, propertyValue, start );
        Reader reader = new Reader( db, propertyKey, propertyValue, start, readerDelay );

        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        Future<?> readResult;
        Future<?> writeResult;
        try
        {
            writeResult = executor.submit( writer );
            readResult = executor.submit( reader );

            start.set( true );
        }
        finally
        {
            executor.shutdown();
            executor.awaitTermination( 20, TimeUnit.SECONDS );
        }

        assertNull( writeResult.get() );
        assertNull( readResult.get() );
    }

    private static class Writer implements Runnable
    {
        final GraphDatabaseService db;
        final String propertyKey;
        final String propertyValue;
        final AtomicBoolean start;

        Writer( GraphDatabaseService db, String propertyKey, String propertyValue, AtomicBoolean start )
        {
            this.db = db;
            this.propertyKey = propertyKey;
            this.propertyValue = propertyValue;
            this.start = start;
        }

        @Override
        public void run()
        {
            while ( !start.get() )
            {
                // spin
            }
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                node.setProperty( propertyKey, propertyValue );
                db.index().forNodes( INDEX_NAME ).add( node, propertyKey, propertyValue );
                tx.success();
            }
        }
    }

    private static class Reader implements Runnable
    {
        final GraphDatabaseService db;
        final String propertyKey;
        final String propertyValue;
        final AtomicBoolean start;
        private final int delay;

        Reader( GraphDatabaseService db, String propertyKey, String propertyValue, AtomicBoolean start, int delay )
        {
            this.db = db;
            this.propertyKey = propertyKey;
            this.propertyValue = propertyValue;
            this.start = start;
            this.delay = delay;
        }

        @Override
        public void run()
        {
            while ( !start.get() )
            {
                // spin
            }
            sleep();
            try ( Transaction tx = db.beginTx() )
            {
                // it is acceptable to either see a node with correct property or not see it at all
                Node node = db.index().forNodes( INDEX_NAME ).get( propertyKey, propertyValue ).getSingle();
                if ( node != null )
                {
                    assertEquals( propertyValue, node.getProperty( propertyKey ) );
                }
                tx.success();
            }
        }

        private void sleep()
        {
            try
            {
                Thread.sleep( delay );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }
    }
}
