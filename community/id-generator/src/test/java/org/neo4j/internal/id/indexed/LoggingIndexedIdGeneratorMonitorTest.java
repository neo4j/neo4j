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
package org.neo4j.internal.id.indexed;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.Integer.min;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.io.ByteUnit.MebiByte;

@TestDirectoryExtension
class LoggingIndexedIdGeneratorMonitorTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    @Test
    void shouldLogAndDumpAllTypesOfCalls() throws IOException
    {
        // given
        File file = directory.file( "file" );
        FakeClock clock = Clocks.fakeClock();
        int timeStep = 100;
        try ( LoggingIndexedIdGeneratorMonitor monitor = new LoggingIndexedIdGeneratorMonitor( fs, file, clock, 10, MebiByte, 1, DAYS ) )
        {
            // when (making all logging calls)
            clock.forward( timeStep, MILLISECONDS );
            monitor.opened( 98, 99 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.allocatedFromHigh( 1 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.allocatedFromReused( 2 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.bridged( 3 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.cached( 4 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.checkpoint( 5, 6 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.markedAsDeletedAndFree( 7 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.markedAsUsed( 8 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.markedAsDeleted( 9 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.markedAsFree( 10 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.markedAsReserved( 11 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.markedAsUnreserved( 12 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.normalized( 13 );
            clock.forward( timeStep, MILLISECONDS );
            monitor.clearingCache();
            clock.forward( timeStep, MILLISECONDS );
            monitor.clearedCache();
            clock.forward( timeStep, MILLISECONDS );
        }

        // then
        LoggingIndexedIdGeneratorMonitor.Dumper dumper = mock( LoggingIndexedIdGeneratorMonitor.Dumper.class );
        LoggingIndexedIdGeneratorMonitor.dump( fs, file, dumper );
        long time = 0;
        verify( dumper ).typeAndTwoIds( LoggingIndexedIdGeneratorMonitor.Type.OPENED, time += timeStep, 98, 99 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.ALLOCATE_HIGH, time += timeStep, 1 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.ALLOCATE_REUSED, time += timeStep, 2 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.BRIDGED, time += timeStep, 3 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.CACHED, time += timeStep, 4 );
        verify( dumper ).typeAndTwoIds( LoggingIndexedIdGeneratorMonitor.Type.CHECKPOINT, time += timeStep, 5, 6 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.MARK_DELETED_AND_FREE, time += timeStep, 7 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.MARK_USED, time += timeStep, 8 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.MARK_DELETED, time += timeStep, 9 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.MARK_FREE, time += timeStep, 10 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.MARK_RESERVED, time += timeStep, 11 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.MARK_UNRESERVED, time += timeStep, 12 );
        verify( dumper ).typeAndId( LoggingIndexedIdGeneratorMonitor.Type.NORMALIZED, time += timeStep, 13 );
        verify( dumper ).type( LoggingIndexedIdGeneratorMonitor.Type.CLEARING_CACHE, time += timeStep);
        verify( dumper ).type( LoggingIndexedIdGeneratorMonitor.Type.CLEARED_CACHE, time += timeStep);
        verify( dumper ).type( LoggingIndexedIdGeneratorMonitor.Type.CLOSED, time += timeStep);
    }

    @Test
    void shouldRotateAndPrune()
    {
        // given
        long sizeOfOneEntry = LoggingIndexedIdGeneratorMonitor.HEADER_SIZE + Long.BYTES;
        File file = directory.file( "file" );
        FakeClock clock = Clocks.fakeClock();
        int entriesPerFile = 10;

        // when
        try ( LoggingIndexedIdGeneratorMonitor monitor =
                new LoggingIndexedIdGeneratorMonitor( fs, file, clock, sizeOfOneEntry * entriesPerFile, ByteUnit.Byte, 3500, MILLISECONDS ) )
        {
            for ( int i = 0; i < 10; i++ )
            {
                for ( int j = 0; j < entriesPerFile; j++ )
                {
                    monitor.markedAsUsed( 0 );
                }
                clock.forward( 1, SECONDS );
                monitor.markSessionDone();
                File parentFile = file.getAbsoluteFile().getParentFile();
                File[] files = parentFile.listFiles( pathname -> pathname.getName().startsWith( file.getName() + '-' ) );
                assertEquals( min( i + 1, 4 ), files.length );
            }
        }
    }

    @Test
    void shouldLogAllConcurrentCallsWhileRotatingAndPruning() throws IOException
    {
        // given
        File file = directory.file( "file" );
        int numberOfThreads = 4;
        int idsPerThread = 1_000;
        try ( LoggingIndexedIdGeneratorMonitor monitor = new LoggingIndexedIdGeneratorMonitor( fs, file, Clocks.nanoClock(), 1, KibiByte, 1, DAYS ) )
        {
            // when
            Race race = new Race();
            race.addContestants( numberOfThreads, id -> () ->
            {
                long nextId = id * idsPerThread;
                for ( int i = 0; i < idsPerThread / 10; i++ )
                {
                    for ( int j = 0; j < 10; j++ )
                    {
                        monitor.markedAsUsed( nextId++ );
                    }
                    // This is the call that checks rotate and pruning
                    monitor.markSessionDone();
                }
            }, 1 );
            race.goUnchecked();
        }

        // then
        BitSet ids = new BitSet();
        LoggingIndexedIdGeneratorMonitor.Dumper dumper = new LoggingIndexedIdGeneratorMonitor.Dumper()
        {
            @Override
            public void file( File file )
            {
            }

            @Override
            public void type( LoggingIndexedIdGeneratorMonitor.Type type, long time )
            {
            }

            @Override
            public void typeAndId( LoggingIndexedIdGeneratorMonitor.Type type, long time, long id )
            {
                int intId = toIntExact( id );
                assertFalse( ids.get( intId ) );
                ids.set( intId );
            }

            @Override
            public void typeAndTwoIds( LoggingIndexedIdGeneratorMonitor.Type type, long time, long id1, long id2 )
            {
            }
        };
        LoggingIndexedIdGeneratorMonitor.dump( fs, file, dumper );
        int totalNumberOfIds = idsPerThread * numberOfThreads;
        for ( int id = 0; id < totalNumberOfIds; id++ )
        {
            assertTrue( ids.get( id ) );
        }
        assertFalse( ids.get( totalNumberOfIds ) );
    }

    @Test
    void shouldDumpLogFilesInCorrectOrder() throws IOException
    {
        // given
        File file = directory.file( "file" );
        FakeClock clock = Clocks.fakeClock();
        int numberOfIds = 100;
        try ( LoggingIndexedIdGeneratorMonitor monitor = new LoggingIndexedIdGeneratorMonitor( fs, file, clock, 100, ByteUnit.Byte, 1, SECONDS ) )
        {
            for ( int i = 0; i < numberOfIds; i++ )
            {
                monitor.allocatedFromHigh( i );
                clock.forward( 50, MILLISECONDS );
                monitor.markSessionDone();
            }
        }

        // when/then
        MutableLong lastId = new MutableLong( -1 );
        LoggingIndexedIdGeneratorMonitor.Dumper dumper = new LoggingIndexedIdGeneratorMonitor.Dumper()
        {
            private long lastFileMillis = -1;
            private boolean lastFileWasTheBaseFile;

            @Override
            public void file( File dumpFile )
            {
                assertFalse( lastFileWasTheBaseFile );
                long timestamp = LoggingIndexedIdGeneratorMonitor.millisOf( dumpFile );
                if ( lastFileMillis != -1 )
                {
                    assertTrue( timestamp > lastFileMillis );
                }
                lastFileMillis = timestamp;
                if ( dumpFile.equals( file ) )
                {
                    lastFileWasTheBaseFile = true;
                }
            }

            @Override
            public void type( LoggingIndexedIdGeneratorMonitor.Type type, long time )
            {
            }

            @Override
            public void typeAndId( LoggingIndexedIdGeneratorMonitor.Type type, long time, long id )
            {
                if ( lastId.longValue() != -1 )
                {
                    assertTrue( id > lastId.longValue() );
                }
                lastId.setValue( id );
            }

            @Override
            public void typeAndTwoIds( LoggingIndexedIdGeneratorMonitor.Type type, long time, long id1, long id2 )
            {
            }
        };
        LoggingIndexedIdGeneratorMonitor.dump( fs, file, dumper );
        assertEquals( numberOfIds - 1, lastId.getValue() );
    }
}
