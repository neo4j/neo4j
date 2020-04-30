/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.internal.event;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

class NodePropertyEntryView implements PropertyEntry<Node>
{
    static final long SHALLOW_SIZE = shallowSizeOfInstance( NodePropertyEntryView.class );

    private final InternalTransaction internalTransaction;
    private final long nodeId;
    private final String key;
    private final Value newValue;
    private final Value oldValue;

    NodePropertyEntryView( InternalTransaction internalTransaction, long nodeId, String key,
            Value newValue, Value oldValue )
    {
        this.internalTransaction = internalTransaction;
        this.nodeId = nodeId;
        this.key = key;
        this.newValue = newValue;
        this.oldValue = oldValue;
    }

    @Override
    public Node entity()
    {
        return new NodeEntity( internalTransaction, nodeId );
    }

    @Override
    public String key()
    {
        return key;
    }

    @Override
    public Object previouslyCommittedValue()
    {
        return oldValue.asObjectCopy();
    }

    @Override
    public Object value()
    {
        if ( newValue == null || newValue == Values.NO_VALUE )
        {
            throw new IllegalStateException( "This property has been removed, it has no value anymore: " + this );
        }
        return newValue.asObjectCopy();
    }

    @Override
    public String toString()
    {
        return "NodePropertyEntryView{" +
                "nodeId=" + nodeId +
                ", key='" + key + '\'' +
                ", newValue=" + newValue +
                ", oldValue=" + oldValue +
                '}';
    }
}
