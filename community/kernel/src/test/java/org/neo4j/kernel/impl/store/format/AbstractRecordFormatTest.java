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
package org.neo4j.kernel.impl.store.format;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.RecordGenerators.Generator;
import org.neo4j.kernel.impl.store.id.BatchingIdSequence;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.lang.System.currentTimeMillis;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public abstract class AbstractRecordFormatTest
{
    private static final int PAGE_SIZE = (int) kibiBytes( 1 );

    // Whoever is hit first
    private static final long TEST_ITERATIONS = 100_000;
    private static final long TEST_TIME = 1000;
    private static final int DATA_SIZE = 100;
    protected static final long NULL = Record.NULL_REFERENCE.intValue();

    private static final RandomRule random = new RandomRule();
    private static final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private static final PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final TestName name = new TestName();

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule( pageCacheRule ).around( fsRule ).around( random );
    private static PageCache pageCache;

    public RecordKeys keys = FullyCoveringRecordKeys.INSTANCE;

    private final RecordFormats formats;
    private final int entityBits;
    private final int propertyBits;
    private RecordGenerators generators;

    protected AbstractRecordFormatTest( RecordFormats formats, int entityBits, int propertyBits )
    {
        this.formats = formats;
        this.entityBits = entityBits;
        this.propertyBits = propertyBits;
    }

    @BeforeClass
    public static void setupPageCache()
    {
        pageCache = pageCacheRule.getPageCache( fsRule.get() );
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
        try ( PagedFile storeFile = pageCache.map( new File( "store-" + name.getMethodName() ), PAGE_SIZE, CREATE ) )
        {
            RecordFormat<R> format = formatSupplier.get();
            RecordKey<R> key = keySupplier.get();
            Generator<R> generator = generatorSupplier.get();
            int recordSize = format.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
            BatchingIdSequence idSequence = new BatchingIdSequence( random.nextBoolean() ?
                    idSureToBeOnTheNextPage( PAGE_SIZE, recordSize ) : 10 );

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
                    readAndVerifyRecord( written, read, format, key, storeFile, recordSize );
                    idSequence.reset();
                }
                catch ( Throwable t )
                {
                    Exceptions.setMessage( t, t.getMessage() + " : written:" + written + ", read:" + read +
                            ", seed:" + random.seed() + ", iteration:" + i );
                    throw t;
                }
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
                format.read( read, cursor, NORMAL, recordSize );
            }
            while ( cursor.shouldRetry() );
            assertWithinBounds( written, cursor, "reading" );
            cursor.checkAndClearCursorException();

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
            format.write( record, cursor, recordSize );
            assertWithinBounds( record, cursor, "writing" );
        }
    }

    private <R extends AbstractBaseRecord> void assertWithinBounds( R record, PageCursor cursor, String operation )
    {
        if ( cursor.checkAndClearBoundsFlag() )
        {
            fail( "Out-of-bounds when " + operation + " record " + record );
        }
    }

    private void assertedNext( PageCursor cursor ) throws IOException
    {
        assertTrue( cursor.next() );
    }

    private long idSureToBeOnTheNextPage( int pageSize, int recordSize )
    {
        return (pageSize + 100) / recordSize;
    }
}
