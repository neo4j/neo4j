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
package org.neo4j.io.pagecache.harness;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheTestSupport;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.randomharness.PageCountRecordFormat;
import org.neo4j.io.pagecache.randomharness.Phase;
import org.neo4j.io.pagecache.randomharness.RandomPageCacheTestHarness;
import org.neo4j.io.pagecache.randomharness.RecordFormat;
import org.neo4j.io.pagecache.randomharness.StandardRecordFormat;
import org.neo4j.resources.Profiler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.ProfilerExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.randomharness.Command.FlushCache;
import static org.neo4j.io.pagecache.randomharness.Command.FlushFile;
import static org.neo4j.io.pagecache.randomharness.Command.MapFile;
import static org.neo4j.io.pagecache.randomharness.Command.ReadMulti;
import static org.neo4j.io.pagecache.randomharness.Command.ReadRecord;
import static org.neo4j.io.pagecache.randomharness.Command.UnmapFile;
import static org.neo4j.io.pagecache.randomharness.Command.WriteMulti;
import static org.neo4j.io.pagecache.randomharness.Command.WriteRecord;

@ExtendWith( {TestDirectoryExtension.class, ProfilerExtension.class} )
abstract class PageCacheHarnessTest<T extends PageCache> extends PageCacheTestSupport<T>
{
    @Inject
    public TestDirectory directory;

    @Inject
    public Profiler profiler;

    @RepeatedTest( 10 )
    void readsAndWritesMustBeMutuallyConsistent()
    {
        assertTimeout( ofMillis( SEMI_LONG_TIMEOUT_MILLIS ), () ->
        {
            int filePageCount = 100;
            try ( RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness() )
            {
                harness.disableCommands( FlushCache, FlushFile, MapFile, UnmapFile );
                harness.setCommandProbabilityFactor( ReadRecord, 0.5 );
                harness.setCommandProbabilityFactor( WriteRecord, 0.5 );
                harness.setConcurrencyLevel( 8 );
                harness.setFilePageCount( filePageCount );
                harness.setInitialMappedFiles( 1 );
                harness.setVerification( filesAreCorrectlyWrittenVerification( new StandardRecordFormat(), filePageCount ) );
                harness.run( SEMI_LONG_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS );
            }
        } );
    }

    @Test
    void concurrentPageFaultingMustNotPutInterleavedDataIntoPages()
    {
        assertTimeout( ofMillis( LONG_TIMEOUT_MILLIS ), () ->
        {
            final int filePageCount = 11;
            final RecordFormat recordFormat = new PageCountRecordFormat();
            try ( RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness() )
            {
                harness.setConcurrencyLevel( 11 );
                harness.setUseAdversarialIO( false );
                harness.setCachePageCount( 3 );
                harness.setFilePageCount( filePageCount );
                harness.setInitialMappedFiles( 1 );
                harness.setCommandCount( 10000 );
                harness.setRecordFormat( recordFormat );
                harness.setFileSystem( fs );
                harness.useProfiler( profiler );
                harness.disableCommands( FlushCache, FlushFile, MapFile, UnmapFile, WriteRecord, WriteMulti );
                harness.setPreparation( ( cache, fs, filesTouched ) ->
                {
                    File file = filesTouched.iterator().next();
                    try ( PagedFile pf = cache.map( file, cache.pageSize() );
                          PageCursor cursor = pf.io( 0, PF_SHARED_WRITE_LOCK ) )
                    {
                        for ( int pageId = 0; pageId < filePageCount; pageId++ )
                        {
                            cursor.next();
                            recordFormat.fillWithRecords( cursor );
                        }
                    }
                } );

                harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
            }
        } );
    }

    @Test
    void concurrentFlushingMustNotPutInterleavedDataIntoFile()
    {
        assertTimeout( ofMillis( LONG_TIMEOUT_MILLIS ), () ->
        {
            final RecordFormat recordFormat = new StandardRecordFormat();
            final int filePageCount = 2_000;
            try ( RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness() )
            {
                harness.setConcurrencyLevel( 16 );
                harness.setUseAdversarialIO( false );
                harness.setCachePageCount( filePageCount / 2 );
                harness.setFilePageCount( filePageCount );
                harness.setInitialMappedFiles( 3 );
                harness.setCommandCount( 15_000 );
                harness.setFileSystem( fs );
                harness.disableCommands( MapFile, UnmapFile, ReadRecord, ReadMulti );
                harness.setVerification( filesAreCorrectlyWrittenVerification( recordFormat, filePageCount ) );

                harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
            }
        } );
    }

    @Test
    void concurrentFlushingWithMischiefMustNotPutInterleavedDataIntoFile()
    {
        assertTimeout( ofMillis( LONG_TIMEOUT_MILLIS ), () ->
        {
            final RecordFormat recordFormat = new StandardRecordFormat();
            final int filePageCount = 2_000;
            try ( RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness() )
            {
                harness.setConcurrencyLevel( 16 );
                harness.setUseAdversarialIO( true );
                harness.setMischiefRate( 0.5 );
                harness.setFailureRate( 0.0 );
                harness.setErrorRate( 0.0 );
                harness.setCachePageCount( filePageCount / 2 );
                harness.setFilePageCount( filePageCount );
                harness.setInitialMappedFiles( 3 );
                harness.setCommandCount( 15_000 );
                harness.setFileSystem( fs );
                harness.disableCommands( MapFile, UnmapFile, ReadRecord, ReadMulti );
                harness.setVerification( filesAreCorrectlyWrittenVerification( recordFormat, filePageCount ) );

                harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
            }
        } );
    }

    @Test
    void concurrentFlushingWithFailuresMustNotPutInterleavedDataIntoFile()
    {
        assertTimeout( ofMillis( LONG_TIMEOUT_MILLIS ), () ->
        {
            final RecordFormat recordFormat = new StandardRecordFormat();
            final int filePageCount = 2_000;
            try ( RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness() )
            {
                harness.setConcurrencyLevel( 16 );
                harness.setUseAdversarialIO( true );
                harness.setMischiefRate( 0.0 );
                harness.setFailureRate( 0.5 );
                harness.setErrorRate( 0.0 );
                harness.setCachePageCount( filePageCount / 2 );
                harness.setFilePageCount( filePageCount );
                harness.setInitialMappedFiles( 3 );
                harness.setCommandCount( 15_000 );
                harness.setFileSystem( fs );
                harness.disableCommands( MapFile, UnmapFile, ReadRecord, ReadMulti );
                harness.setVerification( filesAreCorrectlyWrittenVerification( recordFormat, filePageCount ) );

                harness.run( LONG_TIMEOUT_MILLIS, MILLISECONDS );
            }
        } );
    }

    private Phase filesAreCorrectlyWrittenVerification( final RecordFormat recordFormat, final int filePageCount )
    {
        return ( cache, fs1, filesTouched ) ->
        {
            for ( File file : filesTouched )
            {
                try ( PagedFile pf = cache.map( file, cache.pageSize() );
                      PageCursor cursor = pf.io( 0, PF_SHARED_READ_LOCK ) )
                {
                    for ( int pageId = 0; pageId < filePageCount && cursor.next(); pageId++ )
                    {
                        try
                        {
                            recordFormat.assertRecordsWrittenCorrectly( cursor );
                        }
                        catch ( Throwable th )
                        {
                            th.addSuppressed( new Exception( "pageId = " + pageId ) );
                            throw th;
                        }
                    }
                }
                try ( StoreChannel channel = fs1.open( file, OpenMode.READ ) )
                {
                    recordFormat.assertRecordsWrittenCorrectly( file, channel );
                }
            }
        };
    }
}
