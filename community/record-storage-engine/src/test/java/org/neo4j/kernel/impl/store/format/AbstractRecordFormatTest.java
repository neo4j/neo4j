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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.internal.id.BatchingIdSequence;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.RecordGenerators.Generator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.System.currentTimeMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.Exceptions.withMessage;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@SuppressWarnings( "AbstractClassWithoutAbstractMethods" )
@ExtendWith( {RandomExtension.class, SuppressOutputExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
public abstract class AbstractRecordFormatTest
{
    // Whoever is hit first
    private static final long TEST_ITERATIONS = 100_000;
    private static final long TEST_TIME = 1000;
    private static final int DATA_SIZE = 100;
    protected static final long NULL = Record.NULL_REFERENCE.intValue();

    @Inject
    private RandomRule random;

    public RecordKeys keys = FullyCoveringRecordKeys.INSTANCE;

    private final RecordFormats formats;
    private final int entityBits;
    private final int propertyBits;
    private RecordGenerators generators;
    private String testName;

    protected AbstractRecordFormatTest( RecordFormats formats, int entityBits, int propertyBits )
    {
        this.formats = formats;
        this.entityBits = entityBits;
        this.propertyBits = propertyBits;
    }

    @BeforeEach
    public void before( TestInfo testInfo )
    {
        testName = testInfo.getDisplayName();
        generators = new LimitedRecordGenerators( random.randomValues(), entityBits, propertyBits, 40, 16, -1 );
    }

    @Test
    public void node() throws Exception
    {
        verifyWriteAndRead( formats::node, generators::node, keys::node, true );
    }

    @Test
    public void relationship() throws Exception
    {
        verifyWriteAndRead( formats::relationship, generators::relationship, keys::relationship, true );
    }

    @Test
    public void property() throws Exception
    {
        verifyWriteAndRead( formats::property, generators::property, keys::property, false );
    }

    @Test
    public void relationshipGroup() throws Exception
    {
        verifyWriteAndRead( formats::relationshipGroup, generators::relationshipGroup, keys::relationshipGroup, false );
    }

    @Test
    public void relationshipTypeToken() throws Exception
    {
        verifyWriteAndRead( formats::relationshipTypeToken, generators::relationshipTypeToken, keys::relationshipTypeToken, false );
    }

    @Test
    public void propertyKeyToken() throws Exception
    {
        verifyWriteAndRead( formats::propertyKeyToken, generators::propertyKeyToken, keys::propertyKeyToken, false );
    }

    @Test
    public void labelToken() throws Exception
    {
        verifyWriteAndRead( formats::labelToken, generators::labelToken, keys::labelToken, false );
    }

    @Test
    public void dynamic() throws Exception
    {
        verifyWriteAndRead( formats::dynamic, generators::dynamic, keys::dynamic, false );
    }

    private <R extends AbstractBaseRecord> void verifyWriteAndRead( Supplier<RecordFormat<R>> formatSupplier, Supplier<Generator<R>> generatorSupplier,
            Supplier<RecordKey<R>> keySupplier, boolean assertPostReadOffset ) throws IOException
    {
        // GIVEN
        RecordFormat<R> format = formatSupplier.get();
        RecordKey<R> key = keySupplier.get();
        Generator<R> generator = generatorSupplier.get();
        int recordSize = format.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
        BatchingIdSequence idSequence = new BatchingIdSequence( 1 );
        // WHEN
        PageCursor cursor = ByteArrayPageCursor.wrap( recordSize );
        long time = currentTimeMillis();
        long endTime = time + TEST_TIME;
        long i = 0;
        for ( ; i < TEST_ITERATIONS && currentTimeMillis() < endTime; i++ )
        {
            R written = generator.get( recordSize, format, random.nextLong( 1, format.getMaxId() ) );
            verifyWriteAndReadRecord( assertPostReadOffset, format, key, recordSize, idSequence, cursor, i, written );
        }
    }

    private <R extends AbstractBaseRecord> void verifyWriteAndReadRecord( boolean assertPostReadOffset, RecordFormat<R> format, RecordKey<R> key,
            int recordSize, BatchingIdSequence idSequence, PageCursor cursor, long i, R written ) throws IOException
    {
        R read = format.newRecord();
        R read2 = format.newRecord();
        try
        {
            writeRecord( written, format, cursor, recordSize, idSequence, true );
            readAndVerifyRecord( written, read, format, key, cursor, recordSize, assertPostReadOffset );
            writeRecord( read, format, cursor, recordSize, idSequence, false );
            readAndVerifyRecord( read, read2, format, key, cursor, recordSize, assertPostReadOffset );
            idSequence.reset();
        }
        catch ( Throwable t )
        {
            StringBuilder sb = new StringBuilder( String.valueOf( t.getMessage() ) ).append( System.lineSeparator() );
            sb.append( "Initially written:         " ).append( written ).append( System.lineSeparator() );
            sb.append( "Read back:                 " ).append( read ).append( System.lineSeparator() );
            sb.append( "Wrote and read back again: " ).append( read2 ).append( System.lineSeparator() );
            sb.append( "Seed:                      " ).append( random.seed() ).append( System.lineSeparator() );
            sb.append( "Iteration:                 " ).append( i ).append( System.lineSeparator() );
            withMessage( t, sb.toString() );
            throw t;
        }
    }

    private <R extends AbstractBaseRecord> void readAndVerifyRecord( R written, R read, RecordFormat<R> format, RecordKey<R> key, PageCursor cursor,
            int recordSize, boolean assertPostReadOffset ) throws IOException
    {
        read.setId( written.getId() );

            /*
             Retry loop is needed here because format does not handle retries on the primary cursor.
             Same retry is done on the store level in {@link org.neo4j.kernel.impl.store.CommonAbstractStore}
             */
        do
        {
            cursor.setOffset( 0 );
            format.read( read, cursor, NORMAL, recordSize, 1 );
        }
        while ( cursor.shouldRetry() );
        assertWithinBounds( written, cursor, "reading" );
        if ( assertPostReadOffset )
        {
            assertEquals( recordSize, cursor.getOffset(), "Cursor is positioned on first byte of next record after a read" );
        }
        cursor.checkAndClearCursorException();

        // THEN
        assertEquals( written.inUse(), read.inUse() );
        if ( written.inUse() )
        {
            assertEquals( written.getId(), read.getId() );
            assertEquals( written.getSecondaryUnitId(), read.getSecondaryUnitId() );
            key.assertRecordsEquals( written, read );
        }
    }

    private <R extends AbstractBaseRecord> void writeRecord( R record, RecordFormat<R> format, PageCursor cursor, int recordSize, BatchingIdSequence idSequence,
            boolean prepare ) throws IOException
    {
        if ( prepare && record.inUse() )
        {
            format.prepare( record, recordSize, idSequence );
        }

        cursor.setOffset( 0 );
        format.write( record, cursor, recordSize, 1 );
        assertWithinBounds( record, cursor, "writing" );
    }

    private <R extends AbstractBaseRecord> void assertWithinBounds( R record, PageCursor cursor, String operation )
    {
        if ( cursor.checkAndClearBoundsFlag() )
        {
            fail( "Out-of-bounds when " + operation + " record " + record );
        }
    }
}
