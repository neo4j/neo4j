/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.format.highlimit.v30.RelationshipGroupRecordFormatV3_0;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.NULL;

public class RelationshipGroupRecordFormatTest
{

    private RelationshipGroupRecordFormat recordFormat;
    private FixedLinkedStubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @Before
    public void setUp()
    {
        recordFormat = new RelationshipGroupRecordFormat();
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
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, 0, randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useFixedReferenceFormatWhenOneOfTheReferencesIsMissing() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );

        verifyRecordsWithPoisonedReference( source, target, NULL );
    }

    @Test
    public void useVariableLengthFormatWhenOneOfTheReferencesReferenceTooBig() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );

        verifyRecordsWithPoisonedReference( source, target, 1L << (Integer.SIZE + 2) );
    }

    @Test
    public void useVariableLengthFormatWhenRecordSizeIsTooSmall() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, 0, randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target, RelationshipGroupRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( "Record should use variable length reference if format record is too small.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, 0, randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target, RelationshipGroupRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( "Record should use fixed reference if can fit in format record.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void readSingleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        RelationshipGroupRecord oldFormatRecord = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord newFormatRecord = new RelationshipGroupRecord( 1 );
        oldFormatRecord.initialize( true, 0, randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeRecordWithOldFormat( oldFormatRecord );

        assertFalse( "This should be single unit record.", oldFormatRecord.hasSecondaryUnitId() );
        assertFalse( "Old format is not aware about fixed references.", oldFormatRecord.isUseFixedReferences() );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, RelationshipGroupRecordFormat.RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    @Test
    public void readDoubleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        RelationshipGroupRecord oldFormatRecord = new RelationshipGroupRecord( 0 );
        RelationshipGroupRecord newFormatRecord = new RelationshipGroupRecord( 0 );
        oldFormatRecord.initialize( true, 0, bigReference(), bigReference(),
                bigReference(), bigReference(), bigReference());

        writeRecordWithOldFormat( oldFormatRecord );

        assertTrue( "This should be double unit record.", oldFormatRecord.hasSecondaryUnitId() );
        assertFalse( "Old format is not aware about fixed references.", oldFormatRecord.isUseFixedReferences() );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, RelationshipGroupRecordFormat.RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    private void writeRecordWithOldFormat( RelationshipGroupRecord oldFormatRecord ) throws IOException
    {
        int oldRecordSize = RelationshipGroupRecordFormatV3_0.RECORD_SIZE;
        RelationshipGroupRecordFormatV3_0 recordFormatV30 = new RelationshipGroupRecordFormatV3_0();
        recordFormatV30.prepare( oldFormatRecord, oldRecordSize, idSequence );
        recordFormatV30.write( oldFormatRecord, pageCursor, oldRecordSize );
        pageCursor.setOffset( 0 );
    }

    private void verifyRecordsWithPoisonedReference( RelationshipGroupRecord source, RelationshipGroupRecord target,
            long poisonedReference ) throws IOException
    {
        boolean nullPoisoned = poisonedReference == BaseHighLimitRecordFormat.NULL;
        int differentReferences = 5;
        List<Long> references = buildReferenceList( differentReferences, poisonedReference );
        for ( int i = 0; i < differentReferences; i++ )
        {
            pageCursor.setOffset( 0 );
            Iterator<Long> iterator = references.iterator();

            source.initialize( true, 0, iterator.next(), iterator.next(), iterator.next(), iterator.next(),
                    iterator.next() );

            writeReadRecord( source, target );

            if ( nullPoisoned )
            {
                assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
            }
            else
            {
                assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
            }
            verifySameReferences( source, target );
            Collections.rotate( references, 1 );
        }
    }

    private List<Long> buildReferenceList( int differentReferences, long poison )
    {
        List<Long> references = new ArrayList<>( differentReferences );
        references.add( poison );
        for ( int i = 1; i < differentReferences; i++ )
        {
            references.add( randomFixedReference() );
        }
        return references;
    }

    private void verifySameReferences( RelationshipGroupRecord recordA, RelationshipGroupRecord recordB )
    {
        assertEquals( "First In references should be equal.", recordA.getFirstIn(), recordB.getFirstIn() );
        assertEquals( "First Loop references should be equal.", recordA.getFirstLoop(), recordB.getFirstLoop() );
        assertEquals( "First Out references should be equal.", recordA.getFirstOut(), recordB.getFirstOut() );
        assertEquals( "Next references should be equal.", recordA.getNext(), recordB.getNext() );
        assertEquals( "Prev references should be equal.", recordA.getPrev(), recordB.getPrev() );
        assertEquals( "Owning node references should be equal.", recordA.getOwningNode(), recordB.getOwningNode() );
    }

    private void writeReadRecord( RelationshipGroupRecord source, RelationshipGroupRecord target ) throws java.io.IOException
    {
        writeReadRecord( source, target, RelationshipGroupRecordFormat.RECORD_SIZE );
    }

    private void writeReadRecord( RelationshipGroupRecord source, RelationshipGroupRecord target, int recordSize )
            throws IOException
    {
        recordFormat.prepare( source, recordSize, idSequence );
        recordFormat.write( source, pageCursor, recordSize );
        pageCursor.setOffset( 0 );
        recordFormat.read( target, pageCursor, RecordLoad.NORMAL, recordSize );
    }

    private long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + 1) );
    }

    private long bigReference()
    {
        return 1L << 57;
    }

    private long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

}
