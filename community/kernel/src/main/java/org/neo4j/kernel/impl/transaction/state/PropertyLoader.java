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
package org.neo4j.kernel.impl.transaction.state;

import java.util.Collection;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStore;
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
    private final NeoStore neoStore;

    public PropertyLoader( NeoStore neoStore )
    {
        this.neoStore = neoStore;
        this.nodeStore = neoStore.getNodeStore();
        this.relationshipStore = neoStore.getRelationshipStore();
        this.propertyStore = neoStore.getPropertyStore();
    }

    public void nodeLoadProperties( long nodeId, PropertyReceiver receiver )
    {
        NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
        if ( !nodeRecord.inUse() )
        {
            throw new IllegalStateException( "Node[" + nodeId + "] has been deleted in this tx" );
        }
        loadProperties( nodeRecord.getNextProp(), receiver );
    }

    public void nodeLoadProperties( NodeRecord node, PrimitiveLongObjectMap<PropertyRecord> propertiesById, PropertyReceiver receiver )
    {
        loadProperties( node.getNextProp(), propertiesById, receiver );
    }

    public void relLoadProperties( long relId, PropertyReceiver receiver )
    {
        RelationshipRecord relRecord = relationshipStore.getRecord( relId );
        if ( !relRecord.inUse() )
        {
            throw new InvalidRecordException( "Relationship[" + relId + "] not in use" );
        }
        loadProperties( relRecord.getNextProp(), receiver );
    }

    public void graphLoadProperties( PropertyReceiver records )
    {
        loadProperties( neoStore.asRecord().getNextProp(), records );
    }

    private void loadProperties( long nextProp, PropertyReceiver receiver )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp );
        if ( chain != null )
        {
            loadPropertyChain( chain, receiver );
        }
    }

    private void loadProperties( long nextProp, PrimitiveLongObjectMap<PropertyRecord> propertiesById, PropertyReceiver receiver )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp, propertiesById );
        if ( chain != null )
        {
            loadPropertyChain( chain, receiver );
        }
    }

    private void loadPropertyChain( Collection<PropertyRecord> chain, PropertyReceiver receiver )
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
    }
}
