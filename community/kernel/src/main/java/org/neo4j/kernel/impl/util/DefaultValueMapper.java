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
package org.neo4j.kernel.impl.util;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.NodeReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

import static org.neo4j.helpers.collection.Iterators.iteratorsEqual;

public class DefaultValueMapper extends ValueMapper.JavaMapper
{
    private final EmbeddedProxySPI proxySPI;

    public DefaultValueMapper( EmbeddedProxySPI proxySPI )
    {
        this.proxySPI = proxySPI;
    }

    @Override
    public Node mapNode( VirtualNodeValue value )
    {
        assert !(value instanceof NodeReference);

        if ( value instanceof NodeProxyWrappingNodeValue )
        { // this is the back door through which "virtual nodes" slip
            return ((NodeProxyWrappingNodeValue) value).nodeProxy();
        }
        return new NodeProxy( proxySPI, value.id() );
    }

    @Override
    public Relationship mapRelationship( VirtualRelationshipValue value )
    {
        assert !(value instanceof RelationshipReference);

        if ( value instanceof RelationshipProxyWrappingValue )
        { // this is the back door through which "virtual relationships" slip
            return ((RelationshipProxyWrappingValue) value).relationshipProxy();
        }
        return new RelationshipProxy( proxySPI, value.id() );
    }

    @Override
    public Path mapPath( PathValue value )
    {
        if ( value instanceof PathWrappingPathValue )
        {
            return ((PathWrappingPathValue) value).path();
        }
        return new CoreAPIPath( value );
    }

    private <U, V> Iterable<V> asList( U[] values, Function<U,V> mapper )
    {
        return () -> new Iterator<V>()
        {
            private int index;

            @Override
            public boolean hasNext()
            {
                return index < values.length;
            }

            @Override
            public V next()
            {
                return mapper.apply( values[index++] );
            }
        };
    }

    private <U, V> Iterable<V> asReverseList( U[] values, Function<U,V> mapper )
    {
        return () -> new Iterator<V>()
        {
            private int index = values.length - 1;

            @Override
            public boolean hasNext()
            {
                return index >= 0;
            }

            @Override
            public V next()
            {
                return mapper.apply( values[index--] );
            }
        };
    }

    private class CoreAPIPath implements Path
    {
        private final PathValue value;

        CoreAPIPath( PathValue value )
        {
            this.value = value;
        }

        @Override
        public String toString()
        {
            return Paths.defaultPathToStringWithNotInTransactionFallback( this );
        }

        @Override
        public int hashCode()
        {
            return value.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj instanceof CoreAPIPath )
            {
                return value.equals( ((CoreAPIPath) obj).value );
            }
            else if ( obj instanceof Path )
            {
                Path other = (Path) obj;
                if ( value.nodes()[0].id() != other.startNode().getId() )
                {
                    return false;
                }
                return iteratorsEqual( this.relationships().iterator(), other.relationships().iterator() );
            }
            else
            {
                return false;
            }
        }

        @Override
        public Node startNode()
        {
            return mapNode( value.startNode() );
        }

        @Override
        public Node endNode()
        {
            return mapNode( value.endNode() );
        }

        @Override
        public Relationship lastRelationship()
        {
            if ( value.size() == 0 )
            {
                return null;
            }
            else
            {
                return mapRelationship( value.lastRelationship() );
            }
        }

        @Override
        public Iterable<Relationship> relationships()
        {
            return asList( value.relationships(), DefaultValueMapper.this::mapRelationship );
        }

        @Override
        public Iterable<Relationship> reverseRelationships()
        {
            return asReverseList( value.relationships(), DefaultValueMapper.this::mapRelationship );
        }

        @Override
        public Iterable<Node> nodes()
        {
            return asList( value.nodes(), DefaultValueMapper.this::mapNode );
        }

        @Override
        public Iterable<Node> reverseNodes()
        {
            return asReverseList( value.nodes(), DefaultValueMapper.this::mapNode );
        }

        @Override
        public int length()
        {
            return value.size();
        }

        @Override
        public Iterator<PropertyContainer> iterator()
        {
            return new Iterator<PropertyContainer>()
            {
                private final int size = 2 * value.size() + 1;
                private int index;
                private final NodeValue[] nodes = value.nodes();
                private final RelationshipValue[] relationships = value.relationships();

                @Override
                public boolean hasNext()
                {
                    return index < size;
                }

                @Override
                public PropertyContainer next()
                {
                    PropertyContainer propertyContainer;
                    if ( (index & 1) == 0 )
                    {
                        propertyContainer = mapNode( nodes[index >> 1] );
                    }
                    else
                    {
                        propertyContainer = mapRelationship( relationships[index >> 1] );
                    }
                    index++;
                    return propertyContainer;
                }
            };
        }
    }
}

