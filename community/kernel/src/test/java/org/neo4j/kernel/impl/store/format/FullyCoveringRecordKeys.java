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
package org.neo4j.kernel.impl.store.format;

import java.util.Iterator;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class FullyCoveringRecordKeys implements RecordKeys
{
    public static final RecordKeys INSTANCE = new FullyCoveringRecordKeys();

    @Override
    public RecordKey<NodeRecord> node()
    {
        return ( written, read ) ->
        {
            assertEquals( written.getNextProp(), read.getNextProp() );
            assertEquals( written.getNextRel(), read.getNextRel() );
            assertEquals( written.getLabelField(), read.getLabelField() );
            assertEquals( written.isDense(), read.isDense() );
        };
    }

    @Override
    public RecordKey<RelationshipRecord> relationship()
    {
        return ( written, read ) ->
        {
            assertEquals( written.getNextProp(), read.getNextProp() );
            assertEquals( written.getFirstNode(), read.getFirstNode() );
            assertEquals( written.getSecondNode(), read.getSecondNode() );
            assertEquals( written.getType(), read.getType() );
            assertEquals( written.getFirstPrevRel(), read.getFirstPrevRel() );
            assertEquals( written.getFirstNextRel(), read.getFirstNextRel() );
            assertEquals( written.getSecondPrevRel(), read.getSecondPrevRel() );
            assertEquals( written.getSecondNextRel(), read.getSecondNextRel() );
            assertEquals( written.isFirstInFirstChain(), read.isFirstInFirstChain() );
            assertEquals( written.isFirstInSecondChain(), read.isFirstInSecondChain() );
        };
    }

    @Override
    public RecordKey<PropertyRecord> property()
    {
        return new RecordKey<PropertyRecord>()
        {
            @Override
            public void assertRecordsEquals( PropertyRecord written, PropertyRecord read )
            {
                assertEquals( written.getPrevProp(), read.getPrevProp() );
                assertEquals( written.getNextProp(), read.getNextProp() );
                assertEquals( written.isNodeSet(), read.isNodeSet() );
                if ( written.isNodeSet() )
                {
                    assertEquals( written.getNodeId(), read.getNodeId() );
                }
                else
                {
                    assertEquals( written.getRelId(), read.getRelId() );
                }
                assertEquals( written.numberOfProperties(), read.numberOfProperties() );
                Iterator<PropertyBlock> writtenBlocks = written.iterator();
                Iterator<PropertyBlock> readBlocks = read.iterator();
                while ( writtenBlocks.hasNext() )
                {
                    assertTrue( readBlocks.hasNext() );
                    assertBlocksEquals( writtenBlocks.next(), readBlocks.next() );
                }
            }

            private void assertBlocksEquals( PropertyBlock written, PropertyBlock read )
            {
                assertEquals( written.getKeyIndexId(), read.getKeyIndexId() );
                assertEquals( written.getSize(), read.getSize() );
                assertTrue( written.hasSameContentsAs( read ) );
                assertArrayEquals( written.getValueBlocks(), read.getValueBlocks() );
            }
        };
    }

    @Override
    public RecordKey<RelationshipGroupRecord> relationshipGroup()
    {
        return ( written, read ) ->
        {
            assertEquals( written.getType(), read.getType() );
            assertEquals( written.getFirstOut(), read.getFirstOut() );
            assertEquals( written.getFirstIn(), read.getFirstIn() );
            assertEquals( written.getFirstLoop(), read.getFirstLoop() );
            assertEquals( written.getNext(), read.getNext() );
            assertEquals( written.getOwningNode(), read.getOwningNode() );
        };
    }

    @Override
    public RecordKey<RelationshipTypeTokenRecord> relationshipTypeToken()
    {
        return ( written, read ) -> assertEquals( written.getNameId(), read.getNameId() );
    }

    @Override
    public RecordKey<PropertyKeyTokenRecord> propertyKeyToken()
    {
        return ( written, read ) ->
        {
            assertEquals( written.getNameId(), read.getNameId() );
            assertEquals( written.getPropertyCount(), read.getPropertyCount() );
        };
    }

    @Override
    public RecordKey<LabelTokenRecord> labelToken()
    {
        return ( written, read ) -> assertEquals( written.getNameId(), read.getNameId() );
    }

    @Override
    public RecordKey<DynamicRecord> dynamic()
    {
        return ( written, read ) ->
        {
            // Don't assert type, since that's read from the data, and the data in this test
            // is randomly generated. Since we assert that the data is the same then the type
            // is also correct.
            assertEquals( written.getLength(), read.getLength() );
            assertEquals( written.getNextBlock(), read.getNextBlock() );
            assertArrayEquals( written.getData(), read.getData() );
            assertEquals( written.isStartRecord(), read.isStartRecord() );
        };
    }
}
