/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import org.junit.Test;

import static java.util.Arrays.asList;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;

public class NodeRecordTest
{

    @Test
    public void cloneShouldProduceExactCopy() throws Exception
    {
        // Given
        long relId = 1337l;
        long propId = 1338l;
        long inlinedLabels = 12l;

        NodeRecord node = new NodeRecord( 1l, false, relId, propId );
        node.setLabelField( inlinedLabels, asList( new DynamicRecord( 1l ), new DynamicRecord( 2l ) ) );
        node.setInUse( true );

        // When
        NodeRecord clone = node.clone();

        // Then
        assertEquals(node.inUse(), clone.inUse());
        assertEquals(node.getLabelField(), clone.getLabelField());
        assertEquals(node.getNextProp(), clone.getNextProp());
        assertEquals(node.getNextRel(), clone.getNextRel());

        assertThat( clone.getDynamicLabelRecords(), equalTo(node.getDynamicLabelRecords()) );
    }

    @Test
    public void shouldListLabelRecordsInUse() throws Exception
    {
        // Given
        NodeRecord node = new NodeRecord( 1, false, -1, -1 );
        long inlinedLabels = 12l;
        DynamicRecord dynamic1 = dynamicRecord( 1l, true );
        DynamicRecord dynamic2 = dynamicRecord( 2l, true );
        DynamicRecord dynamic3 = dynamicRecord( 3l, true );

        node.setLabelField( inlinedLabels, asList( dynamic1, dynamic2, dynamic3 ) );

        dynamic3.setInUse( false );

        // When
        Iterable<DynamicRecord> usedRecords = node.getUsedDynamicLabelRecords();

        // Then
        assertThat(toList(usedRecords), equalTo(asList( dynamic1, dynamic2 )));
    }

}
