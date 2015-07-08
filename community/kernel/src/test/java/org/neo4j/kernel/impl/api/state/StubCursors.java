/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.api.properties.DefinedProperty;

/**
 * Stub cursors to be used for testing.
 */
public class StubCursors
{
    public static NodeCursor asNodeCursor( final long nodeId,
            final PropertyCursor propertyCursor,
            final LabelCursor labelCursor )
    {
        return new NodeCursor()
        {
            @Override
            public long getId()
            {
                return nodeId;
            }

            @Override
            public LabelCursor labels()
            {
                return labelCursor;
            }

            @Override
            public PropertyCursor properties()
            {
                return propertyCursor;
            }

            @Override
            public RelationshipCursor relationships( Direction direction, int... relTypes )
            {
                throw new UnsupportedOperationException(  );
            }

            @Override
            public RelationshipCursor relationships( Direction direction )
            {
                throw new UnsupportedOperationException(  );
            }

            @Override
            public boolean next()
            {
                return true;
            }

            @Override
            public void close()
            {

            }
        };
    }

    public static RelationshipCursor asRelationshipCursor( final long relId, final int type,
            final long startNode, final long endNode, final PropertyCursor propertyCursor )
    {
        return new RelationshipCursor()
        {
            @Override
            public long getId()
            {
                return relId;
            }

            @Override
            public int getType()
            {
                return type;
            }

            @Override
            public long getStartNode()
            {
                return startNode;
            }

            @Override
            public long getEndNode()
            {
                return endNode;
            }

            @Override
            public long getOtherNode( long nodeId )
            {
                return startNode == nodeId ? endNode : startNode;
            }

            @Override
            public PropertyCursor properties()
            {
                return propertyCursor;
            }

            @Override
            public boolean next()
            {
                return true;
            }

            @Override
            public void close()
            {

            }
        };
    }

    public static LabelCursor asLabelCursor( final Integer... labels )
    {
        return asLabelCursor( Arrays.asList( labels ) );
    }

    public static LabelCursor asLabelCursor( final List<Integer> labels )
    {
        return new LabelCursor()
        {
            int label = -1;
            int idx = 0;

            @Override
            public boolean seek( int labelId )
            {
                for ( int label : labels )
                {
                    if ( label == labelId )
                    {
                        this.label = label;
                        return true;
                    }
                }

                return false;
            }

            @Override
            public int getLabel()
            {
                return label;
            }

            @Override
            public boolean next()
            {
                if ( idx < labels.size() )
                {
                    this.label = labels.get( idx );
                    idx++;
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
                idx = 0;
            }
        };
    }

    public static PropertyCursor asPropertyCursor( final DefinedProperty... properties )
    {
        return new PropertyCursor()
        {
            DefinedProperty property;
            int idx = 0;

            @Override
            public boolean seek( int keyId )
            {
                for ( DefinedProperty property : properties )
                {
                    if ( property.propertyKeyId() == keyId )
                    {
                        this.property = property;
                        return true;
                    }
                }

                return false;
            }

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

            @Override
            public boolean booleanValue()
            {
                return ((Boolean)value());
            }

            @Override
            public long longValue()
            {
                return ((Number)value()).longValue();
            }

            @Override
            public double doubleValue()
            {
                return ((Number)value()).doubleValue();
            }

            @Override
            public String stringValue()
            {
                return value().toString();
            }

            @Override
            public void propertyData( WritableByteChannel channel )
            {

            }

            @Override
            public boolean next()
            {
                if ( idx < properties.length )
                {
                    this.property = properties[idx];
                    idx++;
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
                idx = 0;
            }
        };
    }

}
