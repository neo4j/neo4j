/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.format.highlimit.v300.NodeRecordFormatV3_0_0;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeRecordFormatTest
{

    private NodeRecordFormat recordFormat;
    private FixedLinkedStubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @BeforeEach
    void setUp()
    {
        recordFormat = new NodeRecordFormat();
        pageCursor = new FixedLinkedStubPageCursor( 0, (int) ByteUnit.kibiBytes( 8 ) );
        idSequence = new ConstantIdSequence();
    }

    @AfterEach
    void tearDown()
    {
        pageCursor.close();
    }

    @Test
    void readWriteFixedReferencesRecord() throws Exception
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferencesFormatWhenRelationshipIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, Record.NULL_REFERENCE.byteValue(), 0L );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferencesFormatWhenPropertyIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, Record.NULL_REFERENCE.intValue(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenRelationshipReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, 1L << 37, true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenPropertyReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, 1L << 37, 0L );

        writeReadRecord( source, target );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenRecordSizeIsTooSmall() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target, NodeRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( target.isUseFixedReferences(),
                "Record should use variable length reference if format record is too small." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target, NodeRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference if can fit in format record." );
        verifySameReferences( source, target );
    }

    @Test
    void readSingleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        NodeRecord oldFormatRecord = new NodeRecord( 1 );
        NodeRecord newFormatRecord = new NodeRecord( 1 );
        oldFormatRecord.initialize( true, randomFixedReference(), true, randomFixedReference(), 1L );

        writeRecordWithOldFormat( oldFormatRecord );

        assertFalse( oldFormatRecord.hasSecondaryUnitId(), "This should be single unit record." );
        assertFalse( oldFormatRecord.isUseFixedReferences(), "Old format is not aware about fixed references." );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, NodeRecordFormat.RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    @Test
    void readDoubleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        NodeRecord oldFormatRecord = new NodeRecord( 1 );
        NodeRecord newFormatRecord = new NodeRecord( 1 );
        oldFormatRecord.initialize( true, bigReference(), true, bigReference(), 1L );

        writeRecordWithOldFormat( oldFormatRecord );

        assertTrue( oldFormatRecord.hasSecondaryUnitId(), "This should be double unit record." );
        assertFalse( oldFormatRecord.isUseFixedReferences(), "Old format is not aware about fixed references." );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, NodeRecordFormat.RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    private void writeRecordWithOldFormat( NodeRecord oldFormatRecord ) throws IOException
    {
        int oldRecordSize = NodeRecordFormatV3_0_0.RECORD_SIZE;
        NodeRecordFormatV3_0_0 recordFormatV30 = new NodeRecordFormatV3_0_0();
        recordFormatV30.prepare( oldFormatRecord, oldRecordSize, idSequence );
        recordFormatV30.write( oldFormatRecord, pageCursor, oldRecordSize );
        pageCursor.setOffset( 0 );
    }

    private void verifySameReferences( NodeRecord recordA, NodeRecord recordB )
    {
        assertEquals( recordA.getNextProp(), recordB.getNextProp(), "Next property field should be the same" );
        assertEquals( recordA.getNextRel(), recordB.getNextRel(), "Next relationship field should be the same." );
        assertEquals( recordA.getLabelField(), recordB.getLabelField(), "Label field should be the same" );
    }

    private void writeReadRecord( NodeRecord source, NodeRecord target ) throws java.io.IOException
    {
        writeReadRecord( source, target, NodeRecordFormat.RECORD_SIZE );
    }

    private void writeReadRecord( NodeRecord source, NodeRecord target, int recordSize ) throws java.io.IOException
    {
        recordFormat.prepare( source, recordSize, idSequence );
        recordFormat.write( source, pageCursor, recordSize );
        pageCursor.setOffset( 0 );
        recordFormat.read( target, pageCursor, RecordLoad.NORMAL, recordSize );
    }

    private long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + (Byte.SIZE / 2)) );
    }

    private long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

    private long bigReference()
    {
        return 1L << 57;
    }
}
