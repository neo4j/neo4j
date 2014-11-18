/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

class IndexConsultedPropertyBlockSweeper implements PrimitiveLongVisitor
{
    private int propertyKeyId;
    private final IndexLookup.Index index;
    private final NodeRecord nodeRecord;
    boolean foundExact;
    private PropertyStore propertyStore;
    private PropertyRemover propertyRemover;

    public IndexConsultedPropertyBlockSweeper( int propertyKeyId, IndexLookup.Index index, NodeRecord nodeRecord,
                                               PropertyStore propertyStore, PropertyRemover propertyRemover )
    {
        this.propertyKeyId = propertyKeyId;
        this.index = index;
        this.nodeRecord = nodeRecord;
        this.propertyStore = propertyStore;
        this.propertyRemover = propertyRemover;
        this.foundExact = false;
    }

    @Override
    public void visited( long recordId )
    {
        PropertyRecord record = propertyStore.getRecord( recordId );
        boolean changed = false;

        List<PropertyBlock> blocks = record.getPropertyBlocks();
        ListIterator<PropertyBlock> it = blocks.listIterator();
        while ( it.hasNext() )
        {
            PropertyBlock block = it.next();

            if ( block.getKeyIndexId() == propertyKeyId )
            {
                Object lastPropertyValue = propertyStore.getValue( block );

                try
                {
                    if ( index.contains( nodeRecord.getId(), lastPropertyValue ) && !foundExact )
                    {
                        foundExact = true;
                    }
                    else
                    {
                        it.remove();
                        changed = true;
                    }
                }
                catch ( IOException e )
                {
                    throw new InnerIterationIOException( e );
                }
            }
        }
        if ( changed )
        {
            if ( blocks.isEmpty() )
            {
                propertyRemover.fixUpPropertyLinksAroundUnusedRecord( nodeRecord, record );
            }
            propertyStore.updateRecord( record );
        }
    }
}
