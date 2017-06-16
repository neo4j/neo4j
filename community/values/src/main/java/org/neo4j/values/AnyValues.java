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
package org.neo4j.values;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.values.storable.Values;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.VirtualValueGroup;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.stream.StreamSupport.stream;
import static org.neo4j.values.virtual.VirtualValues.edgeValue;
import static org.neo4j.values.virtual.VirtualValues.list;
import static org.neo4j.values.virtual.VirtualValues.map;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;

@SuppressWarnings( "WeakerAccess" )
public final class AnyValues
{
    /**
     * Default AnyValue comparator. Will correctly compare all storable and virtual values.
     */
    public static final Comparator<AnyValue> COMPARATOR =
            new AnyValueComparator( Values.COMPARATOR, VirtualValueGroup::compareTo );

    /**
     * Creates an AnyValue by doing type instpection. Do not use in production code where performance is important.
     * @param object the object to turned into a AnyValue
     * @return the AnyValue corresponding to object.
     */
    @SuppressWarnings( "unchecked" )
    public static AnyValue of( Object object )
    {
        try
        {
            return Values.of( object );
        }
        catch ( IllegalArgumentException e )
        {
            if ( object instanceof Node )
            {
                return asNodeValue( (Node) object );
            }
            else if ( object instanceof Relationship )
            {
                return asEdgeValue( (Relationship) object );
            }
            else if ( object instanceof Path )
            {
                Path path = (Path) object;
                NodeValue[] nodes = stream( path.nodes().spliterator(), false )
                        .map( AnyValues::asNodeValue ).toArray( NodeValue[]::new );
                EdgeValue[] edges = stream( path.relationships().spliterator(), false )
                        .map( AnyValues::asEdgeValue ).toArray( EdgeValue[]::new );

                return VirtualValues.path( nodes, edges );
            }
            else if ( object instanceof Map<?,?> )
            {
                return asMapValue( (Map<String,Object>) object );
            }
            else if ( object instanceof Collection<?> )
            {
                AnyValue[] anyValues =
                        ((Collection<?>) object).stream().map( AnyValues::of ).toArray( AnyValue[]::new );
                return list( anyValues );
            }
            else
            {
                throw new IllegalArgumentException(
                        String.format( "Cannot convert %s to AnyValue", object.getClass().getName() ) );
            }
        }
    }

    public static NodeValue asNodeValue( Node node )
    {
        TextValue[] values = stream( node.getLabels().spliterator(), false )
                .map( l -> stringValue( l.name() ) ).toArray( TextValue[]::new );
        return nodeValue( node.getId(), values, asMapValue( node.getAllProperties() ) );
    }

    public static EdgeValue asEdgeValue( Relationship rel )
    {
        return edgeValue( rel.getId(), rel.getStartNodeId(), rel.getEndNodeId(),
                stringValue( rel.getType().name() ), asMapValue( rel.getAllProperties() ) );
    }

    public static MapValue asMapValue( Map<String,Object> map )
    {
        HashMap<String,AnyValue> newMap = new HashMap<>( map.size() );
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            newMap.put( entry.getKey(), of( entry.getValue() ) );
        }

        return map( newMap );
    }
}
