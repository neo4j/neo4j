/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualValues;

public class RelationshipProxyWrappingValue extends RelationshipValue
{
    private final Relationship relationship;
    private volatile TextValue type;
    private volatile MapValue properties;
    private volatile NodeValue startNode;
    private volatile NodeValue endNode;

    RelationshipProxyWrappingValue( Relationship relationship )
    {
        super( relationship.getId() );
        this.relationship = relationship;
    }

    public Relationship relationshipProxy()
    {
        return relationship;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        if ( relationship instanceof RelationshipProxy )
        {
            RelationshipProxy proxy = (RelationshipProxy) relationship;
            if ( !proxy.initializeData() )
            {
                // If the relationship has been deleted since it was found by the query, then we'll have to tell the client that their transaction conflicted,
                // and that they need to retry it.
                throw new ReadAndDeleteTransactionConflictException( RelationshipProxy.isDeletedInCurrentTransaction( relationship ) );
            }
        }

        MapValue p;
        try
        {
            p = properties();
        }
        catch ( ReadAndDeleteTransactionConflictException e )
        {
            if ( !e.wasDeletedInThisTransaction() )
            {
                throw e;
            }
            // If it isn't a transient error then the relationship was deleted in the current transaction and we should write an 'empty' relationship.
            p = VirtualValues.EMPTY_MAP;
        }

        if ( id() < 0 )
        {
            writer.writeVirtualRelationshipHack( relationship );
        }

        writer.writeRelationship( id(), startNode().id(), endNode().id(), type(), p );
    }

    @Override
    public NodeValue startNode()
    {
        NodeValue start = startNode;
        if ( start == null )
        {
            synchronized ( this )
            {
                start = startNode;
                if ( start == null )
                {
                    start = startNode = ValueUtils.fromNodeProxy( relationship.getStartNode() );
                }
            }
        }
        return start;
    }

    @Override
    public NodeValue endNode()
    {
        NodeValue end = endNode;
        if ( end == null )
        {
            synchronized ( this )
            {
                end = endNode;
                if ( end == null )
                {
                    end = endNode = ValueUtils.fromNodeProxy( relationship.getEndNode() );
                }
            }
        }
        return end;
    }

    @Override
    public NodeValue otherNode( VirtualNodeValue node )
    {
        if ( node instanceof NodeProxyWrappingNodeValue )
        {
            Node proxy = ((NodeProxyWrappingNodeValue) node).nodeProxy();
            return ValueUtils.fromNodeProxy( relationship.getOtherNode( proxy ) );
        }
        else
        {
           return super.otherNode( node );
        }
    }

    @Override
    public long otherNodeId( long node )
    {
        return relationship.getOtherNodeId( node );
    }

    @Override
    public TextValue type()
    {
        TextValue t = type;
        if ( t == null )
        {
            try
            {
                synchronized ( this )
                {
                    t = type;
                    if ( t == null )
                    {
                        t = type = Values.stringValue( relationship.getType().name() );
                    }
                }
            }
            catch ( IllegalStateException e )
            {
                throw new ReadAndDeleteTransactionConflictException( RelationshipProxy.isDeletedInCurrentTransaction( relationship ), e );
            }
        }
        return t;
    }

    @Override
    public MapValue properties()
    {
        MapValue m = properties;
        if ( m == null )
        {
            try
            {
                synchronized ( this )
                {
                    m = properties;
                    if ( m == null )
                    {
                        m = properties = ValueUtils.asMapValue( relationship.getAllProperties() );
                    }
                }
            }
            catch ( NotFoundException | IllegalStateException e )
            {
                throw new ReadAndDeleteTransactionConflictException( RelationshipProxy.isDeletedInCurrentTransaction( relationship ), e );
            }
        }
        return m;
    }
}

