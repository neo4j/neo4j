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

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

class DuplicatePropertyRemover
{
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;

    DuplicatePropertyRemover( NodeStore nodeStore, PropertyStore propertyStore )
    {
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
    }

    public void fixUpPropertyLinksAroundUnusedRecord( NodeRecord nodeRecord, PropertyRecord duplicateRecord )
    {
        assert !duplicateRecord.iterator().hasNext();
        long headProp = nodeRecord.getNextProp();
        if ( duplicateRecord.getId() == headProp ) {
            nodeRecord.setNextProp( duplicateRecord.getNextProp() );
            nodeStore.updateRecord( nodeRecord );
        }

        long previousRecordId = duplicateRecord.getPrevProp();
        long nextRecordId = duplicateRecord.getNextProp();
        if ( previousRecordId != Record.NO_PREVIOUS_PROPERTY.intValue() )
        {
            PropertyRecord property = propertyStore.getRecord( previousRecordId );
            property.setNextProp( nextRecordId );
            propertyStore.updateRecord( property );
        }
        if ( nextRecordId != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord property = propertyStore.getRecord( nextRecordId );
            property.setPrevProp( previousRecordId );
            propertyStore.updateRecord( property );
        }
    }
}
