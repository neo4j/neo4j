/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestLengthyArrayPacking extends AbstractNeo4jTestCase
{
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
        clearCache();
        assertTrue( Arrays.equals( arrayWhichUnpackedWouldFillTwoDynamicRecords,
                (int[]) node.getProperty( key ) ) );
    }
}
