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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.values.storable.Value;

/**
 * This class filters acceptNode() calls from an index progressor, to assert that exact entries returned from the
 * progressor really match the exact property values. See also org.neo4j.kernel.impl.api.LookupFilter.
 *
 * It works by acting as a man-in-the-middle between outer {@link NodeValueClient client} and inner {@link IndexProgressor}.
 * Interaction goes like:
 *
 * Initialize:
 * <pre><code>
 * client
 *      -- query( client ) ->      filter = new filter(client)
 *                                 filter -- query( filter ) ->        progressor
 *                                 filter <- initialize(progressor) -- progressor
 * client <- initialize(filter) -- filter
 * </code></pre>
 *
 * Progress:
 * <pre><code>
 * client -- next() ->       filter
 *                           filter -- next() ->          progressor
 *                                     <- acceptNode() --
 *                                  -- :false ->
 *                                     <- acceptNode() --
 *                                  -- :false ->
 *                           filter    <- acceptNode() --
 * client <- acceptNode() -- filter
 *        -- :true ->        filter -- :true ->           progressor
 * client <----------------------------------------------
 * </code></pre>
 *
 * Close:
 * <pre><code>
 * client -- close() -> filter
 *                      filter -- close() -> progressor
 * client <---------------------------------
 * </code></pre>
 */
class NodeValueClientFilter implements NodeValueClient, IndexProgressor
{
    private static final Comparator<IndexQuery> ASCENDING_BY_KEY = Comparator.comparingInt( IndexQuery::propertyKeyId );
    private final NodeValueClient target;
    private final DefaultNodeCursor node;
    private final DefaultPropertyCursor property;
    private final IndexQuery[] filters;
    private final Read read;
    private int[] keys;
    private IndexProgressor progressor;

    NodeValueClientFilter(
            NodeValueClient target,
            DefaultNodeCursor node, DefaultPropertyCursor property, Read read, IndexQuery... filters )
    {
        this.target = target;
        this.node = node;
        this.property = property;
        this.filters = filters;
        this.read = read;
        Arrays.sort( filters, ASCENDING_BY_KEY );
    }

    @Override
    public void initialize( SchemaIndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query )
    {
        this.progressor = progressor;
        this.keys = descriptor.schema().getPropertyIds();
        target.initialize( descriptor, this, query );
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        if ( keys != null && values != null )
        {
            return filterByIndexValues( reference, values );
        }
        else
        {
            node.single( reference, read );
            if ( node.next() )
            {
                node.properties( property );
            }
            else
            {
                property.clear();
                return false;
            }
            return filterByCursors( reference, values );
        }
    }

    @Override
    public boolean needsValues()
    {
        return true;
    }

    @Override
    public boolean next()
    {
        return progressor.next();
    }

    @Override
    public void close()
    {
        node.close();
        property.close();
        progressor.close();
    }

    private boolean filterByIndexValues( long reference, Value[] values )
    {
        FILTERS:
        for ( IndexQuery filter : filters )
        {
            for ( int i = 0; i < keys.length; i++ )
            {
                if ( keys[i] == filter.propertyKeyId() )
                {
                    if ( !filter.acceptsValue( values[i] ) )
                    {
                        return false;
                    }
                    continue FILTERS;
                }
            }
            assert false : "Cannot satisfy filter " + filter + " - no corresponding key!";
            return false;
        }
        return target.acceptNode( reference, values );
    }

    private boolean filterByCursors( long reference, Value[] values )
    {
        // note that this way of checking if all filters are matched relies on the node not having duplicate properties
        int accepted = 0;
        PROPERTIES:
        while ( property.next() )
        {
            for ( IndexQuery filter : filters )
            {
                if ( filter.propertyKeyId() == property.propertyKey() )
                {
                    if ( !filter.acceptsValueAt( property ) )
                    {
                        return false;
                    }
                    accepted++;
                }
                else if ( property.propertyKey() < filter.propertyKeyId() )
                {
                    continue PROPERTIES;
                }
            }
        }
        // if not all filters were matched, i.e. accepted < filters.length we reject
        // otherwise we delegate to target
        return accepted >= filters.length && target.acceptNode( reference, values );
    }
}
