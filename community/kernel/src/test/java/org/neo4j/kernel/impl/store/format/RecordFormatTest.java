/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.RecordGenerators.Generator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.RandomRule;
import org.neo4j.unsafe.impl.batchimport.store.BatchingIdSequence;

import static java.lang.System.currentTimeMillis;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@Ignore( "Not a test, a base class for testing formats" )
public abstract class RecordFormatTest
{
    private static final int PAGE_SIZE = 1_024;

    private static final long TEST_ITERATIONS = 100_000;
    private static final boolean PRINT_STATISTICS = false;
    private static final int DATA_SIZE = 100;
    protected static final long NULL = Record.NULL_REFERENCE.intValue();

    @ClassRule
    public static final RandomRule random = new RandomRule();

    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule( false /*true here later*/ );
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( pageCacheRule ).around( fsRule );

    public RecordKeys keys = FullyCoveringRecordKeys.INSTANCE;

    private final RecordFormats formats;
    private final RecordGenerators generators;

    protected RecordFormatTest( RecordFormats formats, RecordGenerators generators )
    {
        this.formats = formats;
        this.generators = generators;
    }

    @Test
    public void node() throws Exception
    {
        verifyWriteAndRead( formats::node, generators::node, keys::node );
    }

    @Test
    public void relationship() throws Exception
    {
        verifyWriteAndRead( formats::relationship, generators::relationship, keys::relationship );
    }

    @Test
    public void property() throws Exception
    {
        verifyWriteAndRead( formats::property, generators::property, keys::property );
    }

    @Test
    public void relationshipGroup() throws Exception
    {
        verifyWriteAndRead( formats::relationshipGroup, generators::relationshipGroup, keys::relationshipGroup );
    }

    @Test
    public void relationshipTypeToken() throws Exception
    {
        verifyWriteAndRead( formats::relationshipTypeToken, generators::relationshipTypeToken,
                keys::relationshipTypeToken );
    }

    @Test
    public void propertyKeyToken() throws Exception
    {
        verifyWriteAndRead( formats::propertyKeyToken, generators::propertyKeyToken, keys::propertyKeyToken );
    }

    @Test
    public void labelToken() throws Exception
    {
        verifyWriteAndRead( formats::labelToken, generators::labelToken, keys::labelToken );
    }

    @Test
    public void dynamic() throws Exception
    {
        verifyWriteAndRead( formats::dynamic, generators::dynamic, keys::dynamic );
    }

    private <R extends AbstractBaseRecord> void verifyWriteAndRead(
            Supplier<RecordFormat<R>> formatSupplier,
            Supplier<Generator<R>> generatorSupplier,
            Supplier<RecordKey<R>> keySupplier ) throws IOException
    {
        // GIVEN
        PageCache pageCache = pageCacheRule.getPageCache( fsRule.get() );
        try ( PagedFile dontUseStoreFile = pageCache.map( new File( "store" ), PAGE_SIZE, CREATE ) )
        {
            long totalUnusedBytesPrimary = 0;
            long totalUnusedBytesSecondary = 0;
            long totalRecordsRequiringSecondUnit = 0;
            RecordFormat<R> format = formatSupplier.get();
            RecordKey<R> key = keySupplier.get();
            Generator<R> generator = generatorSupplier.get();
            int recordSize = format.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
            RecordBoundaryCheckingPagedFile storeFile =
                    new RecordBoundaryCheckingPagedFile( dontUseStoreFile, recordSize );
            BatchingIdSequence idSequence = new BatchingIdSequence( random.nextBoolean() ?
                    idSureToBeOnTheNextPage( PAGE_SIZE, recordSize ) : 10 );
            long smallestUnusedBytesPrimary = recordSize;
            long smallestUnusedBytesSecondary = recordSize;

            // WHEN
            long time = currentTimeMillis();
            long i = 0;
            for ( ; i < TEST_ITERATIONS; i++ )
            {
                R written = generator.get( recordSize, format );
                try
                {
                    // write
                    try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
                    {
                        assertedNext( cursor );
                        if ( written.inUse() )
                        {
                            format.prepare( written, recordSize, idSequence );
                        }

                        int offset = Math.toIntExact( written.getId() * recordSize );
                        cursor.setOffset( offset );
                        format.write( written, cursor, recordSize, storeFile );
                    }
                    long recordsUsedForWriting = storeFile.nextCalls();
                    long unusedBytes = storeFile.unusedBytes();
                    storeFile.resetMeasurements();

                    // read
                    try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
                    {
                        assertedNext( cursor );
                        int offset = Math.toIntExact( written.getId() * recordSize );
                        cursor.setOffset( offset );
                        @SuppressWarnings( "unchecked" )
                        R read = (R) written.clone(); // just to get a new instance
                        format.read( read, cursor, NORMAL, recordSize, storeFile );

                        // THEN
                        if ( written.inUse() )
                        {
                            assertEquals( written.inUse(), read.inUse() );
                            assertEquals( written.getId(), read.getId() );
                            assertEquals( written.getSecondaryUnitId(), read.getSecondaryUnitId() );
                            key.assertRecordsEquals( written, read );
                        }
                        else
                        {
                            assertEquals( written.inUse(), read.inUse() );
                        }
                    }

                    if ( written.inUse() )
                    {
                        assertEquals( recordsUsedForWriting, storeFile.ioCalls() );
                        assertEquals( recordsUsedForWriting, storeFile.nextCalls() );
                        assertEquals( unusedBytes, storeFile.unusedBytes() );

                        // unused access don't really count for "wasted space"
                        if ( recordsUsedForWriting == 1 )
                        {
                            totalUnusedBytesPrimary += unusedBytes;
                            smallestUnusedBytesPrimary = Math.min( smallestUnusedBytesPrimary, unusedBytes );
                        }
                        else
                        {
                            totalUnusedBytesSecondary += unusedBytes;
                            smallestUnusedBytesSecondary = Math.min( smallestUnusedBytesSecondary, unusedBytes );
                        }
                        totalRecordsRequiringSecondUnit += (recordsUsedForWriting > 1 ? 1 : 0);
                    }

                    storeFile.resetMeasurements();
                    idSequence.reset();
                }
                catch ( Throwable t )
                {
                    Exceptions.setMessage( t, t.getMessage() + " : " + written );
                    throw t;
                }
            }
            if ( PRINT_STATISTICS )
            {
                time = currentTimeMillis() - time;
                System.out.printf( "%s%n  %.2f write-read ops/ms%n  %.2f%% required secondary unit%n" +
                        "  %.2f%% wasted primary record space%n" +
                        "  %.2f%% wasted secondary record space%n" +
                        "  %.2f%% wasted total record space%n" +
                        "  %dB smallest primary waste%n" +
                        "  %dB smallest secondary waste%n",
                        format, ((double)i/time), percent( totalRecordsRequiringSecondUnit, i ),
                        percent( totalUnusedBytesPrimary, i * recordSize ),
                        percent( totalUnusedBytesSecondary, i * recordSize ),
                        percent( totalUnusedBytesPrimary + totalUnusedBytesSecondary, i * recordSize ),
                        smallestUnusedBytesPrimary, smallestUnusedBytesPrimary);
            }
        }
    }

    private double percent( long part, long total )
    {
        return 100.0D * part / total;
    }

    private void assertedNext( PageCursor cursor ) throws IOException
    {
        boolean couldDoNext = cursor.next();
        assert couldDoNext;
    }

    private long idSureToBeOnTheNextPage( int pageSize, int recordSize )
    {
        return (pageSize + 100) / recordSize;
    }
}
