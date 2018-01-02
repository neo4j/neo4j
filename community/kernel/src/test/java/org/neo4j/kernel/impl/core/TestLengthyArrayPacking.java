/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.store.PropertyStore.DEFAULT_DATA_BLOCK_SIZE;

public class TestLengthyArrayPacking extends AbstractNeo4jTestCase
{
    private static final String SOME_MIXED_CHARS = "abc421#¤åäö(/&€";
    private static final String SOME_LATIN_1_CHARS = "abcdefghijklmnopqrstuvwxyz";
    @Test
    public void bitPackingOfLengthyArrays() throws Exception
    {
        long arrayRecordsBefore = dynamicArrayRecordsInUse();

        // Store an int array which would w/o packing require two dynamic records
        // 4*40 = 160B (assuming data size of 120B)
        int[] arrayWhichUnpackedWouldFillTwoDynamicRecords = new int[40];
        for ( int i = 0; i < arrayWhichUnpackedWouldFillTwoDynamicRecords.length; i++ )
        {
            arrayWhichUnpackedWouldFillTwoDynamicRecords[i] = i*i;
        }
        Node node = getGraphDb().createNode();
        String key = "the array";
        node.setProperty( key, arrayWhichUnpackedWouldFillTwoDynamicRecords );
        newTransaction();

        // Make sure it only requires one dynamic record
        assertEquals( arrayRecordsBefore+1, dynamicArrayRecordsInUse() );
        assertTrue( Arrays.equals( arrayWhichUnpackedWouldFillTwoDynamicRecords,
                (int[]) node.getProperty( key ) ) );
    }

    // Tests for strings, although the test class name suggests otherwise

    @Test
    public void makeSureLongLatin1StringUsesOneBytePerChar() throws Exception
    {
        String string = stringOfLength( SOME_LATIN_1_CHARS, DEFAULT_DATA_BLOCK_SIZE*2-1 );
        makeSureRightAmountOfDynamicRecordsUsed( string, 2, STRING_RECORD_COUNTER );
    }

    @Test
    public void makeSureLongUtf8StringUsesLessThanTwoBytesPerChar() throws Exception
    {
        String string = stringOfLength( SOME_MIXED_CHARS, DEFAULT_DATA_BLOCK_SIZE+10 );
        makeSureRightAmountOfDynamicRecordsUsed( string, 2, STRING_RECORD_COUNTER );
    }

    @Test
    public void makeSureLongLatin1StringArrayUsesOneBytePerChar() throws Exception
    {
        // Exactly 120 bytes: 5b header + (19+4)*5. w/o compression 5+(19*2 + 4)*5
        String[] stringArray = new String[5];
        for ( int i = 0; i < stringArray.length; i++ ) stringArray[i] = stringOfLength( SOME_LATIN_1_CHARS, 19 );
        makeSureRightAmountOfDynamicRecordsUsed( stringArray, 1, ARRAY_RECORD_COUNTER );
    }

    @Test
    public void makeSureLongUtf8StringArrayUsesLessThanTwoBytePerChar() throws Exception
    {
        String[] stringArray = new String[7];
        for ( int i = 0; i < stringArray.length; i++ ) stringArray[i] = stringOfLength( SOME_MIXED_CHARS, 20 );
        makeSureRightAmountOfDynamicRecordsUsed( stringArray, 2, ARRAY_RECORD_COUNTER );
    }

    private void makeSureRightAmountOfDynamicRecordsUsed( Object value, int expectedAddedDynamicRecords,
            DynamicRecordCounter recordCounter ) throws Exception
    {
        long stringRecordsBefore = recordCounter.count();
        Node node = getGraphDb().createNode();
        node.setProperty( "name", value );
        newTransaction();
        long stringRecordsAfter = recordCounter.count();
        assertEquals( stringRecordsBefore+expectedAddedDynamicRecords, stringRecordsAfter );
    }

    private String stringOfLength( String possibilities, int length )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            builder.append( possibilities.charAt( i%possibilities.length() ) );
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

    private final DynamicRecordCounter ARRAY_RECORD_COUNTER = new ArrayRecordCounter();
    private final DynamicRecordCounter STRING_RECORD_COUNTER = new StringRecordCounter();
}
