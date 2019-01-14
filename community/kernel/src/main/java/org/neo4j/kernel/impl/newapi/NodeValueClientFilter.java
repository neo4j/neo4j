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

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.IOUtils;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * This class filters acceptNode() calls from an index progressor, to assert that exact entries returned from the
 * progressor really match the exact property values. See also org.neo4j.kernel.impl.api.LookupFilter.
 * <p>
 * It works by acting as a man-in-the-middle between outer {@link NodeValueClient client} and inner {@link IndexProgressor}.
 * Interaction goes like:
 * <p>
 * Initialize:
 * <pre><code>
 * client
 *      -- query( client ) ->      filter = new filter(client)
 *                                 filter -- query( filter ) ->        progressor
 *                                 filter <- initialize(progressor) -- progressor
 * client <- initialize(filter) -- filter
 * </code></pre>
 * <p>
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
 * <p>
 * Close:
 * <pre><code>
 * client -- close() -> filter
 *                      filter -- close() -> progressor
 * client <---------------------------------
 * </code></pre>
 */
class NodeValueClientFilter implements NodeValueClient, IndexProgressor
{
    private final NodeValueClient target;
    private final NodeCursor node;
    private final PropertyCursor property;
    private final IndexQuery[] filters;
    private final org.neo4j.internal.kernel.api.Read read;
    private IndexProgressor progressor;

    NodeValueClientFilter( NodeValueClient target, NodeCursor node, PropertyCursor property, Read read, IndexQuery... filters )
    {
        this.target = target;
        this.node = node;
        this.property = property;
        this.filters = filters;
        this.read = read;
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues )
    {
        this.progressor = progressor;
        target.initialize( descriptor, this, query, indexOrder, needsValues );
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        // First filter on these values, which come from the index. Some values will be NO_VALUE, because some indexed values cannot be read back.
        // Those values will have to be read from the store using the propertyCursor and is done in one pass after this loop, if needed.
        int storeLookups = 0;
        if ( values == null )
        {
            // values == null effectively means that all values are NO_VALUE so we certainly need the store lookup here
            for ( IndexQuery filter : filters )
            {
                if ( filter != null )
                {
                    storeLookups++;
                }
            }
        }
        else
        {
            for ( int i = 0; i < filters.length; i++ )
            {
                IndexQuery filter = filters[i];
                if ( filter != null )
                {
                    if ( values[i] == NO_VALUE )
                    {
                        storeLookups++;
                    }
                    else if ( !filter.acceptsValue( values[i] ) )
                    {
                        return false;
                    }
                }
            }
        }

        // If there were one or more NO_VALUE values above then open store cursor and read those values from the store,
        // applying the same filtering as above, but with a loop designed to do only a single pass over the store values,
        // because it's the most expensive part.
        if ( storeLookups > 0 && !acceptByStoreFiltering( reference, storeLookups, values ) )
        {
            return false;
        }
        return target.acceptNode( reference, values );
    }

    private boolean acceptByStoreFiltering( long reference, int storeLookups, Value[] values )
    {
        // Initialize the property cursor scan
        read.singleNode( reference, node );
        if ( !node.next() )
        {
            // This node doesn't exist, therefore it cannot be accepted
            property.close();
            return false;
        }
        node.properties( property );

        while ( storeLookups > 0 && property.next() )
        {
            for ( int i = 0; i < filters.length; i++ )
            {
                IndexQuery filter = filters[i];
                if ( filter != null && (values == null || values[i] == NO_VALUE) && property.propertyKey() == filter.propertyKeyId() )
                {
                    if ( !filter.acceptsValueAt( property ) )
                    {
                        return false;
                    }
                    storeLookups--;
                }
            }
        }
        return storeLookups == 0;
    }

    @Override
    public boolean needsValues()
    {
        // We return needsValues = true to the progressor, since this will enable us to execute the cheaper filterByIndexValues
        // instead of filterByCursors if the progressor can provide values.
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
        IOUtils.close( RuntimeException::new, node, property, progressor );
    }
}
