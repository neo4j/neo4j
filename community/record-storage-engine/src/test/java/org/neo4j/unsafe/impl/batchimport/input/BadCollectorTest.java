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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.io.NullOutputStream.NULL_OUTPUT_STREAM;
import static org.neo4j.test.OtherThreadExecutor.command;
import static org.neo4j.unsafe.impl.batchimport.input.BadCollector.COLLECT_ALL;
import static org.neo4j.unsafe.impl.batchimport.input.BadCollector.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.input.BadCollector.UNLIMITED_TOLERANCE;

public class BadCollectorTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldCollectBadRelationshipsEvenIfThresholdNeverReached() throws IOException
    {
        // given
        int tolerance = 5;

        try ( BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL ) )
        {
            // when
            badCollector.collectBadRelationship( "1", "a", "T", "2", "b", "1" );

            // then
            assertEquals( 1, badCollector.badEntries() );
        }
    }

    @Test
    public void shouldThrowExceptionIfDuplicateNodeTipsUsOverTheToleranceEdge() throws IOException
    {
        // given
        int tolerance = 1;

        try ( BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL ) )
        {
            // when
            collectBadRelationship( badCollector );
            try
            {
                badCollector.collectDuplicateNode( 1, 1, "group" );
                fail( "Should have thrown an InputException" );
            }
            catch ( InputException ignored )
            {
                // then expect to end up here
            }
        }
    }

    @Test
    public void shouldThrowExceptionIfBadRelationshipsTipsUsOverTheToleranceEdge() throws IOException
    {
        // given
        int tolerance = 1;

        try ( BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.COLLECT_ALL ) )
        {
            // when
            badCollector.collectDuplicateNode( 1, 1, "group" );
            try
            {
                collectBadRelationship( badCollector );
                fail( "Should have thrown an InputException" );
            }
            catch ( InputException ignored )
            {
                // then expect to end up here
            }
        }
    }

    @Test
    public void shouldNotCollectBadRelationshipsIfWeShouldOnlyBeCollectingNodes() throws IOException
    {
        // given
        int tolerance = 1;

        try ( BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.DUPLICATE_NODES ) )
        {
            // when
            badCollector.collectDuplicateNode( 1, 1, "group" );
            try
            {
                collectBadRelationship( badCollector );
            }
            catch ( InputException ignored )
            {
                // then expect to end up here
                assertEquals( 1 /* only duplicate node collected */, badCollector.badEntries() );
            }
        }
    }

    @Test
    public void shouldNotCollectBadNodesIfWeShouldOnlyBeCollectingRelationships() throws IOException
    {
        // given
        int tolerance = 1;

        try ( BadCollector badCollector = new BadCollector( badOutputFile(), tolerance, BadCollector.BAD_RELATIONSHIPS ) )
        {
            // when
            collectBadRelationship( badCollector );
            try
            {
                badCollector.collectDuplicateNode( 1, 1, "group" );
            }
            catch ( InputException ignored )
            {
                // then expect to end up here
                assertEquals( 1 /* only duplicate rel collected */, badCollector.badEntries() );
            }
        }
    }

    @Test
    public void shouldCollectUnlimitedNumberOfBadEntriesIfToldTo()
    {
        // GIVEN
        try ( BadCollector collector = new BadCollector( NULL_OUTPUT_STREAM, UNLIMITED_TOLERANCE, COLLECT_ALL ) )
        {
            // WHEN
            int count = 10_000;
            for ( int i = 0; i < count; i++ )
            {
                collector.collectDuplicateNode( i, i, "group" );
            }

            // THEN
            assertEquals( count, collector.badEntries() );
        }
    }

    @Test
    public void skipBadEntriesLogging()
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try ( BadCollector badCollector = new BadCollector( outputStream, 100, COLLECT_ALL, 10, true, NO_MONITOR ) )
        {
            collectBadRelationship( badCollector );
            for ( int i = 0; i < 2; i++ )
            {
                badCollector.collectDuplicateNode( i, i, "group" );
            }
            collectBadRelationship( badCollector );
            badCollector.collectExtraColumns( "a,b,c", 1, "a" );
            assertEquals( "Output stream should not have any reported entries", 0, outputStream.size() );
        }
    }

    @Test
    public void shouldApplyBackPressure() throws Exception
    {
        // given
        int backPressureThreshold = 10;
        BlockableMonitor monitor = new BlockableMonitor();
        try ( OtherThreadExecutor<Void> t2 = new OtherThreadExecutor<>( "T2", null );
              BadCollector badCollector = new BadCollector( NULL_OUTPUT_STREAM, UNLIMITED_TOLERANCE, COLLECT_ALL, backPressureThreshold, false, monitor ) )
        {
            for ( int i = 0; i < backPressureThreshold; i++ )
            {
                badCollector.collectDuplicateNode( i, i, "group" );
            }

            // when
            Future<Object> enqueue = t2.executeDontWait( command( () -> badCollector.collectDuplicateNode( 999, 999, "group" ) ) );
            t2.waitUntilWaiting( waitDetails -> waitDetails.isAt( BadCollector.class, "collect" ) );
            monitor.unblock();

            // then
            enqueue.get();
        }
    }

    private void collectBadRelationship( Collector collector )
    {
        collector.collectBadRelationship( "A", Group.GLOBAL.name(), "TYPE", "B", Group.GLOBAL.name(), "A" );
    }

    private OutputStream badOutputFile() throws IOException
    {
        File badDataPath = new File( "/tmp/foo2" ).getAbsoluteFile();
        FileSystemAbstraction fileSystem = fs.get();
        File badDataFile = badDataFile( fileSystem, badDataPath );
        return fileSystem.openAsOutputStream( badDataFile, true );
    }

    private File badDataFile( FileSystemAbstraction fileSystem, File badDataPath ) throws IOException
    {
        fileSystem.mkdir( badDataPath.getParentFile() );
        fileSystem.create( badDataPath );
        return badDataPath;
    }

    private static class BlockableMonitor implements BadCollector.Monitor
    {
        private final CountDownLatch latch = new CountDownLatch( 1 );

        @Override
        public void beforeProcessEvent()
        {
            try
            {
                latch.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }

        void unblock()
        {
            latch.countDown();
        }
    }
}
