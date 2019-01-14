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
package org.neo4j.values.virtual;

import java.util.Arrays;

import org.neo4j.values.AnyValue;
import org.neo4j.values.VirtualValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;
import static org.neo4j.values.virtual.VirtualValues.emptyMap;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;

@SuppressWarnings( "WeakerAccess" )
public class VirtualValueTestUtil
{
    public static AnyValue toAnyValue( Object o )
    {
        if ( o instanceof AnyValue )
        {
            return (AnyValue) o;
        }
        else
        {
            return Values.of( o );
        }
    }

    public static NodeValue node( long id, String... labels )
    {
        TextValue[] labelValues = new TextValue[labels.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            labelValues[i] = stringValue( labels[i] );
        }
        return nodeValue( id, stringArray( labels ), emptyMap() );
    }

    public static VirtualValue path( VirtualValue... pathElements )
    {
        assert pathElements.length % 2 == 1;
        NodeValue[] nodes = new NodeValue[pathElements.length / 2 + 1];
        RelationshipValue[] rels = new RelationshipValue[pathElements.length / 2];
        nodes[0] = (NodeValue) pathElements[0];
        for ( int i = 1; i < pathElements.length; i += 2 )
        {
            rels[i / 2] = (RelationshipValue) pathElements[i];
            nodes[i / 2 + 1] = (NodeValue) pathElements[i + 1];
        }
        return VirtualValues.path( nodes, rels );
    }

    public static RelationshipValue rel( long id, long start, long end )
    {
        return relationshipValue( id, node( start ), node( end ), stringValue( "T" ), emptyMap() );
    }

    public static ListValue list( Object... objects )
    {
        AnyValue[] values = new AnyValue[objects.length];
        for ( int i = 0; i < objects.length; i++ )
        {
            values[i] = toAnyValue( objects[i] );
        }
        return VirtualValues.list( values );
    }

    public static VirtualValue map( Object... keyOrVal )
    {
        assert keyOrVal.length % 2 == 0;
        String[] keys = new String[keyOrVal.length / 2];
        AnyValue[] values = new AnyValue[keyOrVal.length / 2];
        for ( int i = 0; i < keyOrVal.length; i += 2 )
        {
            keys[i / 2] = (String) keyOrVal[i];
            values[i / 2] = toAnyValue( keyOrVal[i + 1] );
        }
        return VirtualValues.map( keys, values );
    }

    public static NodeValue[] nodes( long... ids )
    {
        return Arrays.stream( ids )
                .mapToObj( id -> nodeValue( id, stringArray( "L" ), emptyMap() ) )
                .toArray( NodeValue[]::new );
    }

    public static RelationshipValue[] relationships( long... ids )
    {
        return Arrays.stream( ids )
                .mapToObj( id -> relationshipValue( id, node( 0L ), node( 1L ), stringValue( "T" ), emptyMap() ) )
                .toArray( RelationshipValue[]::new );
    }
}
