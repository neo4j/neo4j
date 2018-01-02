/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import java.util.Arrays;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Function;
import org.neo4j.function.IntSupplier;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.cursor.DegreeItem;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.Cursors;

/**
 * Stub cursors to be used for testing.
 */
public class StubCursors
{
    public static Cursor<NodeItem> asNodeCursor( final long nodeId )
    {
        return asNodeCursor( nodeId, Cursors.<PropertyItem>empty(), Cursors.<LabelItem>empty() );
    }

    public static Cursor<NodeItem> asNodeCursor( final long... nodeIds)
    {
        NodeItem[] nodeItems = new NodeItem[nodeIds.length];
        for (int i = 0; i < nodeIds.length; i++)
        {
            nodeItems[i] = asNode( nodeIds[i] );
        }
        return Cursors.cursor( nodeItems);
    }

    public static Cursor<NodeItem> asNodeCursor( final long nodeId,
            final Cursor<PropertyItem> propertyCursor,
            final Cursor<LabelItem> labelCursor )
    {
        return Cursors.<NodeItem>cursor( asNode( nodeId, propertyCursor, labelCursor ) );
    }

    public static NodeItem asNode( final long nodeId )
    {
        return asNode( nodeId, Cursors.<PropertyItem>empty(), Cursors.<LabelItem>empty() );
    }

    public static NodeItem asNode( final long nodeId,
            final Cursor<PropertyItem> propertyCursor,
            final Cursor<LabelItem> labelCursor )
    {
        return new NodeItem.NodeItemHelper()
        {
            @Override
            public long id()
            {
                return nodeId;
            }

            @Override
            public Cursor<LabelItem> label( final int labelId )
            {
                return new Cursor<LabelItem>()
                {
                    Cursor<LabelItem> cursor = labels();

                    @Override
                    public boolean next()
                    {
                        while ( cursor.next() )
                        {
                            if ( cursor.get().getAsInt() == labelId )
                            {
                                return true;
                            }
                        }

                        return false;
                    }

                    @Override
                    public void close()
                    {
                        cursor.close();
                    }

                    @Override
                    public LabelItem get()
                    {
                        return cursor.get();
                    }
                };
            }

            @Override
            public Cursor<LabelItem> labels()
            {
                return labelCursor;
            }

            @Override
            public Cursor<PropertyItem> property( final int propertyKeyId )
            {
                return new Cursor<PropertyItem>()
                {
                    Cursor<PropertyItem> cursor = properties();

                    @Override
                    public boolean next()
                    {
                        while ( cursor.next() )
                        {
                            if ( cursor.get().propertyKeyId() == propertyKeyId )
                            {
                                return true;
                            }
                        }

                        return false;
                    }

                    @Override
                    public void close()
                    {
                        cursor.close();
                    }

                    @Override
                    public PropertyItem get()
                    {
                        return cursor.get();
                    }
                };
            }

            @Override
            public Cursor<PropertyItem> properties()
            {
                return propertyCursor;
            }

            @Override
            public Cursor<RelationshipItem> relationships( Direction direction, int... relTypes )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Cursor<RelationshipItem> relationships( Direction direction )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Cursor<IntSupplier> relationshipTypes()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int degree( Direction direction )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public int degree( Direction direction, int relType )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isDense()
            {
                throw new UnsupportedOperationException(  );
            }

            @Override
            public Cursor<DegreeItem> degrees()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static RelationshipItem asRelationship( final long relId, final int type,
            final long startNode, final long endNode, final Cursor<PropertyItem> propertyCursor )
    {
        return new RelationshipItem.RelationshipItemHelper()
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
            public Cursor<PropertyItem> property( final int propertyKeyId )
            {
                return new Cursor<PropertyItem>()
                {
                    Cursor<PropertyItem> cursor = properties();

                    @Override
                    public boolean next()
                    {
                        while ( cursor.next() )
                        {
                            if ( cursor.get().propertyKeyId() == propertyKeyId )
                            {
                                return true;
                            }
                        }

                        return false;
                    }

                    @Override
                    public void close()
                    {
                        cursor.close();
                    }

                    @Override
                    public PropertyItem get()
                    {
                        return cursor.get();
                    }
                };
            }

            @Override
            public Cursor<PropertyItem> properties()
            {
                return propertyCursor;
            }
        };
    }


    public static Cursor<RelationshipItem> asRelationshipCursor( final long relId, final int type,
            final long startNode, final long endNode, final Cursor<PropertyItem> propertyCursor )
    {
        return Cursors.<RelationshipItem>cursor( new RelationshipItem.RelationshipItemHelper()
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
            public Cursor<PropertyItem> properties()
            {
                return propertyCursor;
            }

            @Override
            public Cursor<PropertyItem> property( final int propertyKeyId )
            {
                return new Cursor<PropertyItem>()
                {
                    Cursor<PropertyItem> cursor = properties();

                    @Override
                    public boolean next()
                    {
                        while ( cursor.next() )
                        {
                            if ( cursor.get().propertyKeyId() == propertyKeyId )
                            {
                                return true;
                            }
                        }

                        return false;
                    }

                    @Override
                    public void close()
                    {
                        cursor.close();
                    }

                    @Override
                    public PropertyItem get()
                    {
                        return cursor.get();
                    }
                };
            }
        } );
    }

    public static Cursor<LabelItem> asLabelCursor( final Integer... labels )
    {
        return asLabelCursor( Arrays.asList( labels ) );
    }

    public static Cursor<LabelItem> asLabelCursor( final List<Integer> labels )
    {
        return Cursors.<LabelItem>cursor( Iterables.map( new Function<Integer, LabelItem>()
        {
            @Override
            public LabelItem apply( final Integer integer )
            {
                return new LabelItem()
                {
                    @Override
                    public int getAsInt()
                    {
                        return integer;
                    }
                };
            }
        }, labels ) );
    }

    public static Cursor<PropertyItem> asPropertyCursor( final DefinedProperty... properties )
    {
        return Cursors.cursor( Iterables.map( new Function<DefinedProperty, PropertyItem>()
        {
            @Override
            public PropertyItem apply( final DefinedProperty property )
            {
                return new PropertyItem()
                {
                    @Override
                    public int propertyKeyId()
                    {
                        return property.propertyKeyId();
                    }

                    @Override
                    public Object value()
                    {
                        return property.value();
                    }
                };
            }
        }, Arrays.asList( properties ) ) );
    }
}
