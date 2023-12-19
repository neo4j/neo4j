/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.format.highlimit.v300.PropertyRecordFormatV3_0_0;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class PropertyRecordFormatTest
{
    private static final int DATA_SIZE = 100;
    private static final long TOO_BIG_REFERENCE = 1L << (Integer.SIZE + (Byte.SIZE * 3));

    private PropertyRecordFormat recordFormat;
    private StubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @Before
    public void setUp()
    {
        recordFormat = new PropertyRecordFormat();
        pageCursor = new StubPageCursor( 0, (int) ByteUnit.kibiBytes( 8 ) );
        idSequence = new ConstantIdSequence();
    }

    @After
    public void tearDown()
    {
        pageCursor.close();
    }

    @Test
    public void writeAndReadRecordWithRelativeReferences()
    {
        int recordSize = recordFormat.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
        long recordId = 0xF1F1F1F1F1F1L;
        int recordOffset = pageCursor.getOffset();

        PropertyRecord record = createRecord( recordFormat, recordId );
        recordFormat.write( record, pageCursor, recordSize );

        PropertyRecord recordFromStore = recordFormat.newRecord();
        recordFromStore.setId( recordId  );
        resetCursor( pageCursor, recordOffset );
        recordFormat.read( recordFromStore, pageCursor, RecordLoad.NORMAL, recordSize );

        // records should be the same
        assertEquals( record.getNextProp(), recordFromStore.getNextProp() );
        assertEquals( record.getPrevProp(), recordFromStore.getPrevProp() );

        // now lets try to read same data into a record with different id - we should get different absolute references
        resetCursor( pageCursor, recordOffset );
        PropertyRecord recordWithOtherId = recordFormat.newRecord();
        recordWithOtherId.setId( 1L  );
        recordFormat.read( recordWithOtherId, pageCursor, RecordLoad.NORMAL, recordSize );

        verifyDifferentReferences(record, recordWithOtherId);
    }

    @Test
    public void readWriteFixedReferencesRecord()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference() );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useFixedReferenceFormatWhenNextPropertyIsMissing()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), Record.NULL_REFERENCE.byteValue() );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useFixedReferenceFormatWhenPreviousPropertyIsMissing()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, Record.NULL_REFERENCE.intValue(), randomFixedReference() );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useVariableLengthFormatWhenPreviousPropertyReferenceTooBig()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, TOO_BIG_REFERENCE, randomFixedReference() );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useVariableLengthFormatWhenNextPropertyReferenceTooBig()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), TOO_BIG_REFERENCE );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useVariableLengthFormatWhenRecordSizeIsTooSmall()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference() );

        writeReadRecord( source, target, PropertyRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( "Record should use variable length reference if format record is too small.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference() );

        writeReadRecord( source, target, PropertyRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( "Record should use fixed reference if can fit in format record.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void readSingleUnitRecordStoredNotInFixedReferenceFormat()
    {
        PropertyRecord oldFormatRecord = new PropertyRecord( 1 );
        PropertyRecord newFormatRecord = new PropertyRecord( 1 );
        oldFormatRecord.initialize( true, randomFixedReference(), randomFixedReference() );

        writeRecordWithOldFormat( oldFormatRecord );

        assertFalse( "This should be single unit record.", oldFormatRecord.hasSecondaryUnitId() );
        assertFalse( "Old format is not aware about fixed references.", oldFormatRecord.isUseFixedReferences() );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, PropertyRecordFormat.RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    private void writeRecordWithOldFormat( PropertyRecord oldFormatRecord )
    {
        int oldRecordSize = PropertyRecordFormatV3_0_0.RECORD_SIZE;
        PropertyRecordFormatV3_0_0 recordFormatV30 = new PropertyRecordFormatV3_0_0();
        recordFormatV30.prepare( oldFormatRecord, oldRecordSize, idSequence );
        recordFormatV30.write( oldFormatRecord, pageCursor, oldRecordSize );
        pageCursor.setOffset( 0 );
    }

    private void verifySameReferences( PropertyRecord recordA, PropertyRecord recordB )
    {
        assertEquals( recordA.getNextProp(), recordB.getNextProp() );
        assertEquals( recordA.getPrevProp(), recordB.getPrevProp() );
    }

    private void verifyDifferentReferences( PropertyRecord recordA, PropertyRecord recordB )
    {
        assertNotEquals( recordA.getNextProp(), recordB.getNextProp() );
        assertNotEquals( recordA.getPrevProp(), recordB.getPrevProp() );
    }

    private void writeReadRecord( PropertyRecord source, PropertyRecord target )
    {
        writeReadRecord( source, target, PropertyRecordFormat.RECORD_SIZE );
    }

    private void writeReadRecord( PropertyRecord source, PropertyRecord target, int recordSize )
    {
        recordFormat.prepare( source, recordSize, idSequence );
        recordFormat.write( source, pageCursor, recordSize );
        pageCursor.setOffset( 0 );
        recordFormat.read( target, pageCursor, RecordLoad.NORMAL, recordSize );
    }

    private long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + (Byte.SIZE * 2)) );
    }

    private long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

    private void resetCursor( StubPageCursor cursor, int recordOffset )
    {
        cursor.setOffset( recordOffset );
    }

    private PropertyRecord createRecord( PropertyRecordFormat format, long recordId )
    {
        PropertyRecord record = format.newRecord();
        record.setInUse( true );
        record.setId( recordId );
        record.setNextProp( 1L );
        record.setPrevProp( (Integer.MAX_VALUE + 1L) << Byte.SIZE * 3 );
        return record;
    }
}
