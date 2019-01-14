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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
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
        source.initialize( true, randomType(), randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        verifySame( source, target);
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
        source.initialize( true, randomType(), randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target, RelationshipGroupRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( "Record should use variable length reference if format record is too small.", target.isUseFixedReferences() );
        verifySame( source, target);
    }

    @Test
    public void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, randomType(), randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target, RelationshipGroupRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( "Record should use fixed reference if can fit in format record.", target.isUseFixedReferences() );
        verifySame( source, target);
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
            verifySame( source, target );
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

    private void verifySame( RelationshipGroupRecord recordA, RelationshipGroupRecord recordB )
    {
        assertEquals( "Types should be equal.", recordA.getType(), recordB.getType() );
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

    private int randomType()
    {
        return (int) randomReference( 1L << 24 );
    }

    private long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + 1) );
    }

    private long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

}
