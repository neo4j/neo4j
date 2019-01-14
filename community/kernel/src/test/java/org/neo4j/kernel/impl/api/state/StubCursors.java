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
package org.neo4j.kernel.impl.api.state;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.values.storable.Value;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.emptySet;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

/**
 * Stub cursors to be used for testing.
 */
public class StubCursors
{
    private StubCursors()
    {
    }

    public static Cursor<NodeItem> asNodeCursor( long... nodeIds )
    {
        NodeItem[] nodeItems = new NodeItem[nodeIds.length];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeItems[i] = new StubNodeItem( nodeIds[i], -1, emptySet() );
        }
        return cursor( nodeItems );
    }

    public static Cursor<NodeItem> asNodeCursor( long nodeId )
    {
        return asNodeCursor( nodeId, -1 );
    }

    public static Cursor<NodeItem> asNodeCursor( long nodeId, long propertyId )
    {
        return asNodeCursor( nodeId, propertyId, emptySet() );
    }

    public static Cursor<NodeItem> asNodeCursor( long nodeId, PrimitiveIntSet labels )
    {
        return cursor( new StubNodeItem( nodeId, -1, labels ) );
    }

    public static Cursor<NodeItem> asNodeCursor( long nodeId, long propertyId, PrimitiveIntSet labels )
    {
        return cursor( new StubNodeItem( nodeId, propertyId, labels ) );
    }

    private static class StubNodeItem implements NodeItem
    {
        private final long nodeId;
        private final long propertyId;
        private final PrimitiveIntSet labels;

        private StubNodeItem( long nodeId, long propertyId, PrimitiveIntSet labels )
        {
            this.nodeId = nodeId;
            this.propertyId = propertyId;
            this.labels = labels;
        }

        @Override
        public long id()
        {
            return nodeId;
        }

        @Override
        public boolean hasLabel( int labelId )
        {
            return labels.contains( labelId );
        }

        @Override
        public long nextGroupId()
        {
            throw new UnsupportedOperationException( "not supported" );
        }

        @Override
        public long nextRelationshipId()
        {
            throw new UnsupportedOperationException( "not supported" );
        }

        @Override
        public long nextPropertyId()
        {
            return propertyId;
        }

        @Override
        public Lock lock()
        {
            return NO_LOCK;
        }

        @Override
        public PrimitiveIntSet labels()
        {
            return labels;
        }

        @Override
        public boolean isDense()
        {
            throw new UnsupportedOperationException(  );
        }
    }

    public static RelationshipItem relationship( long id, int type, long start, long end )
    {
        return new RelationshipItem()
        {
            @Override
            public long id()
            {
                return id;
            }

            @Override
            public int type()
            {
                return type;
            }

            @Override
            public long startNode()
            {
                return start;
            }

            @Override
            public long endNode()
            {
                return end;
            }

            @Override
            public long otherNode( long nodeId )
            {
                if ( nodeId == start )
                {
                    return end;
                }
                else if ( nodeId == end )
                {
                    return start;
                }
                throw new IllegalStateException();
            }

            @Override
            public long nextPropertyId()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Lock lock()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static Cursor<RelationshipItem> asRelationshipCursor( final long relId, final int type,
            final long startNode, final long endNode, long propertyId )
    {
        return cursor( new RelationshipItem()
        {
            @Override
            public long id()
            {
                return relId;
            }

            @Override
            public int type()
            {
                return type;
            }

            @Override
            public long startNode()
            {
                return startNode;
            }

            @Override
            public long endNode()
            {
                return endNode;
            }

            @Override
            public long otherNode( long nodeId )
            {
                return startNode == nodeId ? endNode : startNode;
            }

            @Override
            public long nextPropertyId()
            {
                return propertyId;
            }

            @Override
            public Lock lock()
            {
                return NO_LOCK;
            }
        } );
    }

    public static PrimitiveIntSet labels( final int... labels )
    {
        return PrimitiveIntCollections.asSet( labels );
    }

    public static Cursor<PropertyItem> asPropertyCursor( final PropertyKeyValue... properties )
    {
        return cursor( map( StubCursors::asPropertyItem, Arrays.asList( properties ) ) );
    }

    private static PropertyItem asPropertyItem( final PropertyKeyValue property )
    {
        return new PropertyItem()
        {
            @Override
            public int propertyKeyId()
            {
                return property.propertyKeyId();
            }

            @Override
            public Value value()
            {
                return property.value();
            }
        };
    }

    @SafeVarargs
    public static <T> Cursor<T> cursor( final T... items )
    {
        return cursor( Iterables.asIterable( items ) );
    }

    public static <T> Cursor<T> cursor( final Iterable<T> items )
    {
        return new Cursor<T>()
        {
            Iterator<T> iterator = items.iterator();

            T current;

            @Override
            public boolean next()
            {
                if ( iterator.hasNext() )
                {
                    current = iterator.next();
                    return true;
                }
                else
                {
                    return false;
                }
            }

            @Override
            public void close()
            {
                iterator = items.iterator();
                current = null;
            }

            @Override
            public T get()
            {
                if ( current == null )
                {
                    throw new IllegalStateException();
                }

                return current;
            }
        };
    }
}
