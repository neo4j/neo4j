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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat.DEFAULT_DATA_BLOCK_SIZE;

class TestLengthyArrayPacking extends AbstractNeo4jTestCase
{
    private static final String SOME_MIXED_CHARS = "abc421#¤åäö(/&€";
    private static final String SOME_LATIN_1_CHARS = "abcdefghijklmnopqrstuvwxyz";
    private final DynamicRecordCounter ARRAY_RECORD_COUNTER = new ArrayRecordCounter();
    private final DynamicRecordCounter STRING_RECORD_COUNTER = new StringRecordCounter();

    @Test
    void bitPackingOfLengthyArrays()
    {
        long arrayRecordsBefore = dynamicArrayRecordsInUse();
        int[] arrayWhichUnpackedWouldFillTwoDynamicRecords = new int[40];
        Node node;
        String key = "the array";

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // Store an int array which would w/o packing require two dynamic records
            // 4*40 = 160B (assuming data size of 120B)
            for ( int i = 0; i < arrayWhichUnpackedWouldFillTwoDynamicRecords.length; i++ )
            {
                arrayWhichUnpackedWouldFillTwoDynamicRecords[i] = i * i;
            }
            node = transaction.createNode();
            node.setProperty( key, arrayWhichUnpackedWouldFillTwoDynamicRecords );

            // Make sure it only requires one dynamic record
            transaction.commit();
        }

        assertEquals( arrayRecordsBefore + 1, dynamicArrayRecordsInUse() );
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertArrayEquals( arrayWhichUnpackedWouldFillTwoDynamicRecords, (int[]) transaction.getNodeById( node.getId() ).getProperty( key ) );
            transaction.commit();
        }
    }

    // Tests for strings, although the test class name suggests otherwise

    @Test
    void makeSureLongLatin1StringUsesOneBytePerChar()
    {
        String string = stringOfLength( SOME_LATIN_1_CHARS, DEFAULT_DATA_BLOCK_SIZE * 2 - 1 );
        makeSureRightAmountOfDynamicRecordsUsed( string, 2, STRING_RECORD_COUNTER );
    }

    @Test
    void makeSureLongUtf8StringUsesLessThanTwoBytesPerChar()
    {
        String string = stringOfLength( SOME_MIXED_CHARS, DEFAULT_DATA_BLOCK_SIZE + 10 );
        makeSureRightAmountOfDynamicRecordsUsed( string, 2, STRING_RECORD_COUNTER );
    }

    @Test
    void makeSureLongLatin1StringArrayUsesOneBytePerChar()
    {
        // Exactly 120 bytes: 5b header + (19+4)*5. w/o compression 5+(19*2 + 4)*5
        String[] stringArray = new String[5];
        for ( int i = 0; i < stringArray.length; i++ )
        {
            stringArray[i] = stringOfLength( SOME_LATIN_1_CHARS, 19 );
        }
        makeSureRightAmountOfDynamicRecordsUsed( stringArray, 1, ARRAY_RECORD_COUNTER );
    }

    @Test
    void makeSureLongUtf8StringArrayUsesLessThanTwoBytePerChar()
    {
        String[] stringArray = new String[7];
        for ( int i = 0; i < stringArray.length; i++ )
        {
            stringArray[i] = stringOfLength( SOME_MIXED_CHARS, 20 );
        }
        makeSureRightAmountOfDynamicRecordsUsed( stringArray, 2, ARRAY_RECORD_COUNTER );
    }

    private void makeSureRightAmountOfDynamicRecordsUsed( Object value, int expectedAddedDynamicRecords,
            DynamicRecordCounter recordCounter )
    {
        long stringRecordsBefore = recordCounter.count();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "name", value );
            transaction.commit();
        }
        long stringRecordsAfter = recordCounter.count();
        assertEquals( stringRecordsBefore + expectedAddedDynamicRecords, stringRecordsAfter );
    }

    private static String stringOfLength( String possibilities, int length )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            builder.append( possibilities.charAt( i % possibilities.length() ) );
        }
        return builder.toString();
    }

    private interface DynamicRecordCounter
    {
        long count();
    }

    private class ArrayRecordCounter implements DynamicRecordCounter
    {
        @Override
        public long count()
        {
            return dynamicArrayRecordsInUse();
        }
    }

    private class StringRecordCounter implements DynamicRecordCounter
    {
        @Override
        public long count()
        {
            return dynamicStringRecordsInUse();
        }
    }
}
