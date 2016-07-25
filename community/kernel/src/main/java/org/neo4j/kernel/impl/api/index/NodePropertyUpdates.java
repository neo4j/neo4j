/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.api.index.NodePropertyUpdate;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

/**
 * Container for all properties updates for one node identified by {@linkplain #nodeId}
 * Used to group node properties updates and and pass in processing chain, so all updates can be processed together.
 */
public class NodePropertyUpdates
{
    private Collection<NodePropertyUpdate> propertyUpdates = new ArrayList<>();
    private long nodeId;

    public NodePropertyUpdates()
    {
    }

    public void reset()
    {
        propertyUpdates.clear();
        nodeId = NO_ID;
    }

    public void initForNodeId( long nodeId )
    {
        if ( containsUpdates() )
        {
            throw new AssertionError( "Please clear updates fom previous node." );
        }
        this.nodeId = nodeId;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public void add( NodePropertyUpdate update )
    {
        propertyUpdates.add( update );
    }

    public void add( int propertyKeyId, Object value, long[] labels )
    {
        if ( nodeId == NO_ID )
        {
            throw new AssertionError( "Please init property updates container for specific node." );
        }
        propertyUpdates.add( NodePropertyUpdate.add( nodeId, propertyKeyId, value, labels ) );
    }

    public boolean containsUpdates()
    {
        return !propertyUpdates.isEmpty();
    }

    public Collection<NodePropertyUpdate> getPropertyUpdates()
    {
        return propertyUpdates;
    }

    public void addAll( Collection<NodePropertyUpdate> propertyUpdates )
    {
        this.propertyUpdates.addAll( propertyUpdates );
    }
}
