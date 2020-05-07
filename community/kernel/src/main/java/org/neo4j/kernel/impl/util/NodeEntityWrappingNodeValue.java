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
package org.neo4j.kernel.impl.util;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.exceptions.StoreFailureException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.AnyValueWriter.EntityMode.REFERENCE;

public class NodeEntityWrappingNodeValue extends NodeValue
{
    static final long SHALLOW_SIZE = shallowSizeOfInstance( NodeEntityWrappingNodeValue.class ) + NodeEntity.SHALLOW_SIZE;

    private final Node node;
    private volatile TextArray labels;
    private volatile MapValue properties;

    NodeEntityWrappingNodeValue( Node node )
    {
        super( node.getId() );
        this.node = node;
    }

    public Node nodeEntity()
    {
        return node;
    }

    @Override
    public <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E
    {
        if ( writer.entityMode() == REFERENCE )
        {
            writer.writeNodeReference( id() );
        }
        else
        {
            TextArray l;
            MapValue p;
            try
            {
                l = labels();
                p = properties();
            }
            catch ( NotFoundException e )
            {
                l = Values.stringArray();
                p = VirtualValues.EMPTY_MAP;
            }
            catch ( StoreFailureException e )
            {
                throw new ReadAndDeleteTransactionConflictException( NodeEntity.isDeletedInCurrentTransaction( node ), e );
            }

            if ( id() < 0 )
            {
                writer.writeVirtualNodeHack( node );
            }

            writer.writeNode( node.getId(), l, p );
        }
    }

    public void populate()
    {
        try
        {
            labels();
            properties();
        }
        catch ( NotFoundException | StoreFailureException e )
        {
            // best effort, cannot do more
        }
    }

    public boolean isPopulated()
    {
        return labels != null && properties != null;
    }

    @Override
    public TextArray labels()
    {
        TextArray l = labels;
        if ( l == null )
        {
            synchronized ( this )
            {
                l = labels;
                if ( l == null )
                {
                    List<String> ls = new ArrayList<>();
                    for ( Label label : node.getLabels() )
                    {
                        ls.add( label.name() );
                    }
                    l = labels = Values.stringArray( ls.toArray( new String[0] ) );

                }
            }
        }
        return l;
    }

    @Override
    public MapValue properties()
    {
        MapValue m = properties;
        if ( m == null )
        {
            synchronized ( this )
            {
                m = properties;
                if ( m == null )
                {
                    m = properties = ValueUtils.asMapValue( node.getAllProperties() );
                }
            }
        }
        return m;
    }

    @Override
    public long estimatedHeapUsage()
    {
        long size = SHALLOW_SIZE;
        if ( labels != null )
        {
            size += labels.estimatedHeapUsage();
        }
        if ( properties != null )
        {
            size += properties.estimatedHeapUsage();
        }
        return size;
    }
}
