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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

class IndexConsultedPropertyBlockSweeper implements PrimitiveLongVisitor<IOException>
{
    private final int propertyKeyId;
    private final IndexLookup.Index index;
    private final NodeRecord nodeRecord;
    boolean foundExact;
    private final PropertyStore propertyStore;
    private final DuplicatePropertyRemover propertyRemover;

    public IndexConsultedPropertyBlockSweeper( int propertyKeyId, IndexLookup.Index index, NodeRecord nodeRecord,
                                               PropertyStore propertyStore, DuplicatePropertyRemover propertyRemover )
    {
        this.propertyKeyId = propertyKeyId;
        this.index = index;
        this.nodeRecord = nodeRecord;
        this.propertyStore = propertyStore;
        this.propertyRemover = propertyRemover;
        this.foundExact = false;
    }

    @Override
    public boolean visited( long propRecordId ) throws IOException
    {
        PropertyRecord record = propertyStore.getRecord( propRecordId );
        boolean changed = false;

        Iterator<PropertyBlock> it = record.iterator();
        while ( it.hasNext() )
        {
            PropertyBlock block = it.next();

            if ( block.getKeyIndexId() == propertyKeyId )
            {
                Object propertyValue = propertyStore.getValue( block );

                if ( !foundExact && index.contains( nodeRecord.getId(), propertyValue ) )
                {
                    foundExact = true;
                }
                else
                {
                    it.remove();
                    changed = true;
                }
            }
        }
        if ( changed )
        {
            if ( record.numberOfProperties() == 0 )
            {
                propertyRemover.fixUpPropertyLinksAroundUnusedRecord( nodeRecord, record );
            }
            propertyStore.updateRecord( record );
        }
        return false;
    }
}
