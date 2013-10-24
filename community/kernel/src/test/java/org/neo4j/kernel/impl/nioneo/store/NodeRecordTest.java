/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class NodeRecordTest
{

    @Test
    public void cloneShouldProduceExactCopy() throws Exception
    {
        // Given
        long relId = 1337l;
        long propId = 1338l;
        long inlinedLabels = 12l;

        NodeRecord node = new NodeRecord( 1l, relId, propId );
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

}
