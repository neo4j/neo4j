/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.values.storable.Value;

/**
 * This class filters acceptNode() calls from an index progressor, to assert that exact entries returned from the
 * progressor really match the exact property values. See also org.neo4j.kernel.impl.api.LookupFilter.
 */
class NodeValueClientFilter implements IndexProgressor.NodeValueClient
{
    private static final Comparator<IndexQuery> ASCENDING_BY_KEY = Comparator.comparingInt( IndexQuery::propertyKeyId );
    private final IndexProgressor.NodeValueClient target;
    private final NodeCursor node;
    private final PropertyCursor property;
    private final IndexQuery[] filters;
    private int[] keys;

    NodeValueClientFilter(
            IndexProgressor.NodeValueClient target,
            NodeCursor node, PropertyCursor property, IndexQuery... filters )
    {
        this.target = target;
        this.node = node;
        this.property = property;
        this.filters = filters;
        Arrays.sort( filters, ASCENDING_BY_KEY );
    }

    @Override
    public void initialize( IndexProgressor progressor, int[] propertyIds )
    {
        this.keys = propertyIds;
        target.initialize( progressor, propertyIds );
    }

    @Override
    public void done()
    {
        node.close();
        property.close();
        target.done();
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
            node.single( reference );
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
        if ( accepted < filters.length )
        {
            return false; // not all filters were matched
        }
        return target.acceptNode( reference, values );
    }
}
