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
import org.neo4j.values.storable.Value;

class IndexCursorFilter implements CursorProgressor.Cursor<IndexState.NodeValue>
{
    private static final Comparator<IndexQuery> ASCENDING_BY_KEY = Comparator.comparingInt( IndexQuery::propertyKeyId );
    private final CursorProgressor.Cursor<IndexState.NodeValue> target;
    private final NodeCursor node;
    private final PropertyCursor property;
    private final IndexQuery[] filters;

    IndexCursorFilter(
            CursorProgressor.Cursor<IndexState.NodeValue> target,
            NodeCursor node, PropertyCursor property, IndexQuery... filters )
    {
        this.target = target;
        this.node = node;
        this.property = property;
        this.filters = filters;
        Arrays.sort( filters, ASCENDING_BY_KEY );
    }

    @Override
    public void empty()
    {
        node.close();
        property.close();
        target.empty();
    }

    @Override
    public void initialize( CursorProgressor<IndexState.NodeValue> progressor )
    {
        target.initialize( new Progressor( progressor, node, property, filters ) );
    }

    private static class Progressor implements CursorProgressor<IndexState.NodeValue>, IndexState.NodeValue
    {
        private final CursorProgressor<IndexState.NodeValue> progressor;
        private final NodeCursor node;
        private final PropertyCursor property;
        private final IndexQuery[] filters;
        private long reference;
        private int[] keys;
        private Value[] values;

        Progressor(
                CursorProgressor<IndexState.NodeValue> progressor,
                NodeCursor node, PropertyCursor property, IndexQuery[] filters )
        {
            this.progressor = progressor;
            this.node = node;
            this.property = property;
            this.filters = filters;
        }

        @Override
        public boolean next( IndexState.NodeValue nodeValue )
        {
            if ( !progressor.next( this ) )
            {
                return false;
            }
            PROPERTIES:
            while ( property.next() )
            {
                for ( IndexQuery filter : filters )
                {
                    if ( filter.propertyKeyId() == property.propertyKey() )
                    {
                        if ( !filter.acceptsValueAt( property ) )
                        {
                            // this will reset the property cursor to the properties of the next node
                            if ( !progressor.next( this ) )
                            {
                                return false;
                            }
                            // so all we have to do is continue in order to inspect the next one
                            continue PROPERTIES;
                        }
                    }
                    else if ( property.propertyKey() < filter.propertyKeyId() )
                    {
                        continue PROPERTIES;
                    }
                }
            }
            // when we get here, we know that we are on a node where all filters are accepted
            nodeValue.node( reference, keys, values );
            return true;
        }

        @Override
        public void close()
        {
            node.close();
            property.close();
            progressor.close();
        }

        @Override
        public void node( long reference, int[] keys, Value[] values )
        {
            // it's a bit iffy that we have to put these things into fields, but ok enough.
            this.reference = reference;
            this.keys = keys;
            this.values = values;
            node.single( reference );
            if ( node.next() )
            {
                node.properties( property );
            }
            else
            {
                property.clear();
            }
        }

        @Override
        public void done()
        {
        }
    }
}
