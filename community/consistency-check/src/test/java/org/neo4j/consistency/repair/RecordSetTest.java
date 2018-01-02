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
package org.neo4j.consistency.repair;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.assertEquals;

public class RecordSetTest
{
    @Test
    public void toStringShouldPlaceEachRecordOnItsOwnLine() throws Exception
    {
        // given
        NodeRecord record1 = new NodeRecord( 1, false, 1, 1 );
        NodeRecord record2 = new NodeRecord( 2, false, 2, 2 );
        RecordSet<NodeRecord> set = new RecordSet<NodeRecord>();
        set.add( record1 );
        set.add( record2 );

        // when
        String string = set.toString();

        // then
        String[] lines = string.split( "\n" );
        assertEquals(4, lines.length);
        assertEquals( "[", lines[0] );
        assertEquals( record1.toString() + ",", lines[1] );
        assertEquals( record2.toString() + ",", lines[2] );
        assertEquals( "]", lines[3] );
    }
}
