/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.store.format.highlimit;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.format.highlimit.v300.NodeRecordFormatV3_0_0;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeRecordFormatTest
{

    private NodeRecordFormat recordFormat;
    private FixedLinkedStubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @Before
    public void setUp()
    {
        recordFormat = new NodeRecordFormat();
        pageCursor = new FixedLinkedStubPageCursor( 0, (int) ByteUnit.kibiBytes( 8 ) );
        idSequence = new ConstantIdSequence();
    }

    @After
    public void tearDown()
    {
        pageCursor.close();
    }

    @Test
    public void readWriteFixedReferencesRecord() throws Exception
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useFixedReferencesFormatWhenRelationshipIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, Record.NULL_REFERENCE.byteValue(), 0L );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useFixedReferencesFormatWhenPropertyIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, Record.NULL_REFERENCE.intValue(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useVariableLengthFormatWhenRelationshipReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, 1L << 37, true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useVariableLengthFormatWhenPropertyReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, 1L << 37, 0L );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useVariableLengthFormatWhenRecordSizeIsTooSmall() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target, NodeRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( "Record should use variable length reference if format record is too small.",
                target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target, NodeRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( "Record should use fixed reference if can fit in format record.", target.isUseFixedReferences() );
        verifySameReferences( source, target );
    }

    @Test
    public void readSingleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        NodeRecord oldFormatRecord = new NodeRecord( 1 );
        NodeRecord newFormatRecord = new NodeRecord( 1 );
        oldFormatRecord.initialize( true, randomFixedReference(), true, randomFixedReference(), 1L );

        writeRecordWithOldFormat( oldFormatRecord );

        assertFalse( "This should be single unit record.", oldFormatRecord.hasSecondaryUnitId() );
        assertFalse( "Old format is not aware about fixed references.", oldFormatRecord.isUseFixedReferences() );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, NodeRecordFormat.RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    @Test
    public void readDoubleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        NodeRecord oldFormatRecord = new NodeRecord( 1 );
        NodeRecord newFormatRecord = new NodeRecord( 1 );
        oldFormatRecord.initialize( true, bigReference(), true, bigReference(), 1L );

        writeRecordWithOldFormat( oldFormatRecord );

        assertTrue( "This should be double unit record.", oldFormatRecord.hasSecondaryUnitId() );
        assertFalse( "Old format is not aware about fixed references.", oldFormatRecord.isUseFixedReferences() );

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
        assertEquals( "Next property field should be the same", recordA.getNextProp(), recordB.getNextProp() );
        assertEquals( "Next relationship field should be the same.", recordA.getNextRel(), recordB.getNextRel() );
        assertEquals( "Label field should be the same", recordA.getLabelField(), recordB.getLabelField() );
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
