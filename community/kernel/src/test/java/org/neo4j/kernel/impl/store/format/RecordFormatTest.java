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

import org.junit.Before;
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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@Ignore( "Not a test, a base class for testing formats" )
public abstract class RecordFormatTest
{
    private static final int PAGE_SIZE = (int) kibiBytes( 1 );

    // Whoever is hit first
    private static final long TEST_ITERATIONS = 20_000;
    private static final long TEST_TIME = 500;
    private static final long PRINT_RESULTS_THRESHOLD = SECONDS.toMillis( 1 );
    private static final int DATA_SIZE = 100;
    protected static final long NULL = Record.NULL_REFERENCE.intValue();

    @ClassRule
    public static final RandomRule random = new RandomRule();

    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( pageCacheRule ).around( fsRule );

    public RecordKeys keys = FullyCoveringRecordKeys.INSTANCE;

    private final RecordFormats formats;
    private final int entityBits;
    private final int propertyBits;
    private RecordGenerators generators;

    protected RecordFormatTest( RecordFormats formats, int entityBits, int propertyBits )
    {
        this.formats = formats;
        this.entityBits = entityBits;
        this.propertyBits = propertyBits;
    }

    @Before
    public void before()
    {
        generators = new LimitedRecordGenerators( random.randoms(), entityBits, propertyBits, 40, 16, -1 );
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
            long endTime = time + TEST_TIME;
            long i = 0;
            for ( ; i < TEST_ITERATIONS && currentTimeMillis() < endTime; i++ )
            {
                R written = generator.get( recordSize, format, i % 5 );
                R read = format.newRecord();
                try
                {
                    writeRecord( written, format, storeFile, recordSize, idSequence );

                    long recordsUsedForWriting = storeFile.nextCalls();
                    long unusedBytes = storeFile.unusedBytes();
                    storeFile.resetMeasurements();

                    readAndVerifyRecord( written, read, format, key, storeFile, recordSize );

                    if ( written.inUse() )
                    {
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
                    Exceptions.setMessage( t, t.getMessage() + " : written:" + written + ", read:" + read +
                            ", seed:" + random.seed() + ", iteration:" + i );
                    throw t;
                }
            }
            time = currentTimeMillis() - time;
            if ( time >= PRINT_RESULTS_THRESHOLD )
            {
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

    private <R extends AbstractBaseRecord> void readAndVerifyRecord( R written, R read, RecordFormat<R> format,
            RecordKey<R> key, PagedFile storeFile, int recordSize ) throws IOException
    {
        try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
        {
            assertedNext( cursor );
            read.setId( written.getId() );

            /**
             Retry loop is needed here because format does not handle retries on the primary cursor.
             Same retry is done on the store level in {@link org.neo4j.kernel.impl.store.CommonAbstractStore}
             */
            int offset = Math.toIntExact( written.getId() * recordSize );
            do
            {
                cursor.setOffset( offset );
                format.read( read, cursor, NORMAL, recordSize, storeFile );
            }
            while ( cursor.shouldRetry() );
            assertFalse( "Out-of-bounds when reading record " + written, cursor.checkAndClearBoundsFlag() );

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
    }

    private <R extends AbstractBaseRecord> void writeRecord( R record, RecordFormat<R> format, PagedFile storeFile,
            int recordSize, BatchingIdSequence idSequence ) throws IOException
    {
        try ( PageCursor cursor = storeFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            assertedNext( cursor );
            if ( record.inUse() )
            {
                format.prepare( record, recordSize, idSequence );
            }

            int offset = Math.toIntExact( record.getId() * recordSize );
            cursor.setOffset( offset );
            format.write( record, cursor, recordSize, storeFile );
            assertFalse( "Out-of-bounds when writing record " + record, cursor.checkAndClearBoundsFlag() );
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
