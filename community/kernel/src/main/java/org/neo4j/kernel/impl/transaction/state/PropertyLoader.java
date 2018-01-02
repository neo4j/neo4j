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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState.PropertyReceiver;

public class PropertyLoader
{
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final PropertyStore propertyStore;
    private final MetaDataStore metaDataStore;

    public PropertyLoader( NeoStores neoStores )
    {
        this.nodeStore = neoStores.getNodeStore();
        this.metaDataStore = neoStores.getMetaDataStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.propertyStore = neoStores.getPropertyStore();
    }

    public <RECEIVER extends PropertyReceiver> RECEIVER nodeLoadProperties( long nodeId, RECEIVER receiver )
    {
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        loadProperties( nodeRecord.getNextProp(), receiver );
        return receiver;
    }

    public <RECEIVER extends PropertyReceiver> RECEIVER nodeLoadProperties( NodeRecord node,
            PrimitiveLongObjectMap<PropertyRecord> propertiesById, RECEIVER receiver )
    {
        return loadProperties( node.getNextProp(), propertiesById, receiver );
    }

    public <RECEIVER extends PropertyReceiver> RECEIVER relLoadProperties( long relId, RECEIVER receiver )
    {
        RelationshipRecord relRecord = relationshipStore.getRecord( relId );
        return loadProperties( relRecord.getNextProp(), receiver );
    }

    public <RECEIVER extends PropertyReceiver> RECEIVER graphLoadProperties( RECEIVER records )
    {
        return loadProperties( metaDataStore.asRecord().getNextProp(), records );
    }

    private <RECEIVER extends PropertyReceiver> RECEIVER loadProperties( long nextProp, RECEIVER receiver )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp );
        return receivePropertyChain( receiver, chain );
    }

    private <RECEIVER extends PropertyReceiver> RECEIVER loadProperties( long nextProp,
            PrimitiveLongObjectMap<PropertyRecord> propertiesById, RECEIVER receiver )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp, propertiesById );
        return receivePropertyChain( receiver, chain );
    }

    private <RECEIVER extends PropertyReceiver> RECEIVER receivePropertyChain( RECEIVER receiver,
            Collection<PropertyRecord> chain )
    {
        if ( chain != null )
        {
            for ( PropertyRecord propRecord : chain )
            {
                for ( PropertyBlock propBlock : propRecord )
                {
                    receiver.receive( propBlock.newPropertyData( propertyStore ), propRecord.getId() );
                }
            }
        }
        return receiver;
    }
}
