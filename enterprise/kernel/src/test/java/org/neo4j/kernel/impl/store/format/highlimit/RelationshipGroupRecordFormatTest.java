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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.NULL;

class RelationshipGroupRecordFormatTest
{

    private RelationshipGroupRecordFormat recordFormat;
    private FixedLinkedStubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @BeforeEach
    void setUp()
    {
        recordFormat = new RelationshipGroupRecordFormat();
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
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, randomType(), randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySame( source, target);
    }

    @Test
    void useFixedReferenceFormatWhenOneOfTheReferencesIsMissing() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );

        verifyRecordsWithPoisonedReference( source, target, NULL );
    }

    @Test
    void useVariableLengthFormatWhenOneOfTheReferencesReferenceTooBig() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );

        verifyRecordsWithPoisonedReference( source, target, 1L << (Integer.SIZE + 2) );
    }

    @Test
    void useVariableLengthFormatWhenRecordSizeIsTooSmall() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, randomType(), randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target, RelationshipGroupRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( target.isUseFixedReferences(),
                "Record should use variable length reference if format record is too small." );
        verifySame( source, target);
    }

    @Test
    void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        RelationshipGroupRecord source = new RelationshipGroupRecord( 1 );
        RelationshipGroupRecord target = new RelationshipGroupRecord( 1 );
        source.initialize( true, randomType(), randomFixedReference(), randomFixedReference(),
                randomFixedReference(), randomFixedReference(), randomFixedReference());

        writeReadRecord( source, target, RelationshipGroupRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference if can fit in format record." );
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
                assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
            }
            else
            {
                assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
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
        assertEquals( recordA.getType(), recordB.getType(), "Types should be equal." );
        assertEquals( recordA.getFirstIn(), recordB.getFirstIn(), "First In references should be equal." );
        assertEquals( recordA.getFirstLoop(), recordB.getFirstLoop(), "First Loop references should be equal." );
        assertEquals( recordA.getFirstOut(), recordB.getFirstOut(), "First Out references should be equal." );
        assertEquals( recordA.getNext(), recordB.getNext(), "Next references should be equal." );
        assertEquals( recordA.getPrev(), recordB.getPrev(), "Prev references should be equal." );
        assertEquals( recordA.getOwningNode(), recordB.getOwningNode(), "Owning node references should be equal." );
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
