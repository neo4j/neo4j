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

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.offsetForId;
import static org.neo4j.kernel.impl.store.format.BaseRecordFormat.IN_USE_BIT;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.NULL;

public class RelationshipRecordFormatTest
{
    private final RelationshipRecordFormat format = new RelationshipRecordFormat();
    private final int recordSize = format.getRecordSize( NO_STORE_HEADER );
    private ConstantIdSequence idSequence = new ConstantIdSequence();
    private final FixedLinkedStubPageCursor cursor = new FixedLinkedStubPageCursor( 0, (int) ByteUnit.kibiBytes( 4 ) )
    {
        @Override
        public boolean next( long pageId )
        {
            // We're going to use this cursor in an environment where in all genericness this cursor
            // is one that can be moved around to other pages. That's not possible with this stub cursor,
            // however we know that in this test we'll stay on page 0 even if there are calls to next(pageId)
            // which are part of the format code.
            assertEquals( 0, pageId );
            return true;
        }
    };

    @Test
    public void writeAndReadRecordWithRelativeReferences() throws IOException
    {
        long recordId = 0xF1F1F1F1F1F1L;
        int recordOffset = cursor.getOffset();

        RelationshipRecord record = createRecord( format, recordId, false, false );
        RelationshipRecord firstInSecondChain = createRecord( format, recordId, false, true );
        RelationshipRecord firstInFirstChain = createRecord( format, recordId, true, false );
        RelationshipRecord firstInBothChains = createRecord( format, recordId, true, true );

        checkRecord( format, recordSize, cursor, recordId, recordOffset, record );
        checkRecord( format, recordSize, cursor, recordId, recordOffset, firstInSecondChain );
        checkRecord( format, recordSize, cursor, recordId, recordOffset, firstInFirstChain );
        checkRecord( format, recordSize, cursor, recordId, recordOffset, firstInBothChains );
    }

    /*
     * This test acts as a test group for whoever uses BaseHighLimitRecordFormat base class,
     * the logic for marking both units as unused when deleting exists there.
     */
    @Test
    public void shouldMarkBothUnitsAsUnusedWhenDeletingRecordWhichHasSecondaryUnit() throws Exception
    {
        // GIVEN a record which requires two units
        PagedFile storeFile = mock( PagedFile.class );
        when( storeFile.pageSize() ).thenReturn( cursor.getCurrentPageSize() );
        long hugeValue = 1L << 48;
        RelationshipRecord record = new RelationshipRecord( 5 ).initialize( true,
                hugeValue + 1, hugeValue + 2, hugeValue + 3, 4,
                hugeValue + 5, hugeValue + 6, hugeValue + 7, hugeValue + 8, true, true );
        record.setSecondaryUnitId( 17 );
        record.setRequiresSecondaryUnit( true );
        cursor.setOffset( offsetForId( record.getId(), cursor.getCurrentPageSize(), recordSize ) );
        format.write( record, cursor, recordSize );

        // WHEN deleting that record
        record.setInUse( false );
        cursor.setOffset( offsetForId( record.getId(), cursor.getCurrentPageSize(), recordSize ) );
        format.write( record, cursor, recordSize );

        // THEN both units should have been marked as unused
        cursor.setOffset( offsetForId( record.getId(), cursor.getCurrentPageSize(), recordSize ) );
        assertFalse( recordInUse( cursor ) );
        cursor.setOffset( offsetForId( record.getSecondaryUnitId(), cursor.getCurrentPageSize(), recordSize ) );
        assertFalse( recordInUse( cursor ) );
    }

    @Test
    public void readWriteFixedReferencesRecord() throws Exception
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference(), randomFixedReference(), randomShortType(),
                randomFixedReference(), randomFixedReference(), randomFixedReference(), randomFixedReference(),
                true, true );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useVariableLengthFormatWhenTypeIsTooBig() throws IOException
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference(), randomFixedReference(), 1 << 16,
                randomFixedReference(), randomFixedReference(), randomFixedReference(), randomFixedReference(),
                true, true );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length format.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useFixedReferenceFormatWhenTypeIsSmallEnough() throws IOException
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference(), randomFixedReference(), (1 << 16) - 1,
                randomFixedReference(), randomFixedReference(), randomFixedReference(), randomFixedReference(),
                true, true );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useFixedRecordFormatWhenAtLeastOneOfTheReferencesIsMissing() throws IOException
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );

        verifyRecordsWithPoisonedReference( source, target, NULL, randomShortType() );
    }

    @Test
    public void useVariableLengthFormatWhenAtLeastOneOfTheReferencesIsTooBig() throws IOException
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );
        verifyRecordsWithPoisonedReference( source, target, 1L << Integer.SIZE + 5, randomType() );
    }

    @Test
    public void useVariableLengthFormatWhenRecordSizeIsTooSmall() throws IOException
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference(), randomFixedReference(), randomType(),
                randomFixedReference(), randomFixedReference(), randomFixedReference(), randomFixedReference(),
                true, true );

        writeReadRecord( source, target, RelationshipRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( "Record should use variable length reference if format record is too small.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    @Test
    public void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        RelationshipRecord source = new RelationshipRecord( 1 );
        RelationshipRecord target = new RelationshipRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference(), randomFixedReference(), randomShortType(),
                randomFixedReference(), randomFixedReference(), randomFixedReference(), randomFixedReference(),
                true, true );

        writeReadRecord( source, target, RelationshipRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( "Record should use fixed reference if can fit in format record.", target.isUseFixedReferences() );
        verifySameReferences( source, target);
    }

    private void verifyRecordsWithPoisonedReference( RelationshipRecord source, RelationshipRecord target,
            long poisonedReference, int type ) throws IOException
    {
        boolean nullPoison = poisonedReference == NULL;
        // first and second node can't be empty references so excluding them in case if poisoned reference is null
        int differentReferences = nullPoison ? 5 : 7;
        List<Long> references = buildReferenceList( differentReferences, poisonedReference );
        for ( int i = 0; i < differentReferences; i++ )
        {
            cursor.setOffset( 0 );
            Iterator<Long> iterator = references.iterator();
            source.initialize( true, iterator.next(),
                    nullPoison ? randomFixedReference() : iterator.next(),
                    nullPoison ? randomFixedReference() : iterator.next(),
                    type, iterator.next(), iterator.next(), iterator.next(), iterator.next(), true, true );

            writeReadRecord( source, target );

            if ( nullPoison )
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

    private void writeReadRecord( RelationshipRecord source, RelationshipRecord target ) throws java.io.IOException
    {
        writeReadRecord( source, target, recordSize );
    }

    private void writeReadRecord( RelationshipRecord source, RelationshipRecord target, int recordSize ) throws java.io.IOException
    {
        format.prepare( source, recordSize, idSequence );
        format.write( source, cursor, recordSize );
        cursor.setOffset( 0 );
        format.read( target, cursor, RecordLoad.NORMAL, recordSize );
    }

    private boolean recordInUse( StubPageCursor cursor )
    {
        byte header = cursor.getByte();
        return (header & IN_USE_BIT) != 0;
    }

    private void checkRecord( RelationshipRecordFormat format, int recordSize, StubPageCursor cursor,
            long recordId, int recordOffset, RelationshipRecord record ) throws IOException
    {
        format.write( record, cursor, recordSize );

        RelationshipRecord recordFromStore = format.newRecord();
        recordFromStore.setId( recordId  );
        resetCursor( cursor, recordOffset );
        format.read( recordFromStore, cursor, RecordLoad.NORMAL, recordSize );

        // records should be the same
        verifySameReferences( record, recordFromStore );

        // now lets try to read same data into a record with different id - we should get different absolute references
        resetCursor( cursor, recordOffset );
        RelationshipRecord recordWithOtherId = format.newRecord();
        recordWithOtherId.setId( 1L  );
        format.read( recordWithOtherId, cursor, RecordLoad.NORMAL, recordSize );

        assertNotEquals( record.getFirstNextRel(), recordWithOtherId.getFirstNextRel() );
        assertNotEquals( record.getFirstPrevRel(), recordWithOtherId.getFirstPrevRel() );
        assertNotEquals( record.getSecondNextRel(), recordWithOtherId.getSecondNextRel() );
        assertNotEquals( record.getSecondPrevRel(), recordWithOtherId.getSecondPrevRel() );
    }

    private void verifySameReferences( RelationshipRecord record, RelationshipRecord recordFromStore )
    {
        assertEquals( "Types should be equal.", record.getType(), recordFromStore.getType() );
        assertEquals( "First Next references should be equal.", record.getFirstNextRel(), recordFromStore.getFirstNextRel() );
        assertEquals( "First Node references should be equal.", record.getFirstNode(), recordFromStore.getFirstNode() );
        assertEquals( "First Prev Rel references should be equal.", record.getFirstPrevRel(), recordFromStore.getFirstPrevRel() );
        assertEquals( "Second Next Rel references should be equal.", record.getSecondNextRel(), recordFromStore.getSecondNextRel() );
        assertEquals( "Second Node references should be equal.", record.getSecondNode(), recordFromStore.getSecondNode() );
        assertEquals( "Second Prev Rel references should be equal.", record.getSecondPrevRel(), recordFromStore.getSecondPrevRel() );
        assertEquals( "Next Prop references should be equal.", record.getNextProp(), recordFromStore.getNextProp() );
    }

    private void resetCursor( StubPageCursor cursor, int recordOffset )
    {
        cursor.setOffset( recordOffset );
    }

    private RelationshipRecord createRecord( RelationshipRecordFormat format, long recordId,
            boolean firstInFirstChain, boolean firstInSecondChain )
    {
        RelationshipRecord record = format.newRecord();
        record.setInUse( true );
        record.setFirstInFirstChain( firstInFirstChain );
        record.setFirstInSecondChain( firstInSecondChain );
        record.setId( recordId );
        record.setFirstNextRel( 1L );
        record.setFirstNode( 2L );
        record.setFirstPrevRel( 3L );
        record.setSecondNextRel( 4L );
        record.setSecondNode( 5L );
        record.setSecondPrevRel( 6L );
        record.setType( 7 );
        return record;
    }

    private int randomShortType()
    {
        return (int) randomReference( 1L << Short.SIZE );
    }

    private int randomType()
    {
        return (int) randomReference( 1L << 24 );
    }

    private long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + 1 ) );
    }

    private long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }
}
