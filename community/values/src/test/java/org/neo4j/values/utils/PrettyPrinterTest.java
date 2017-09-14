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
package org.neo4j.values.utils;

import org.junit.Test;

import java.util.HashMap;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.EdgeReference;
import org.neo4j.values.virtual.EdgeValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.list;

public class PrettyPrinterTest
{
    @Test
    public void shouldHandleNodeReference()
    {
        // Given
        NodeReference node = VirtualValues.node( 42L );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=42)" ) );
    }

    @Test
    public void shouldHandleNodeValue()
    {
        // Given
        NodeValue node = VirtualValues.nodeValue( 42L, Values.stringArray( "L1", "L2", "L3" ),
                props( "foo", intValue( 42 ), "bar", list( intValue( 1337 ), stringValue( "baz" ) ) ) );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=42 :L1:L2:L3 {bar: [1337, \"baz\"], foo: 42})" ) );
    }

    @Test
    public void shouldHandleNodeValueWithoutLabels()
    {
        // Given
        NodeValue node = VirtualValues.nodeValue( 42L, Values.stringArray(),
                props( "foo", intValue( 42 ), "bar", list( intValue( 1337 ), stringValue( "baz" ) ) ) );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=42 {bar: [1337, \"baz\"], foo: 42})" ) );
    }

    @Test
    public void shouldHandleNodeValueWithoutProperties()
    {
        // Given
        NodeValue node = VirtualValues.nodeValue( 42L, Values.stringArray( "L1", "L2", "L3" ), EMPTY_MAP );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=42 :L1:L2:L3)" ) );
    }

    @Test
    public void shouldHandleNodeValueWithoutLabelsNorProperties()
    {
        // Given
        NodeValue node = VirtualValues.nodeValue( 42L, Values.stringArray(), EMPTY_MAP );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=42)" ) );
    }

    @Test
    public void shouldHandleEdgeReference()
    {
        // Given
        EdgeReference edge = VirtualValues.edge( 42L );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        edge.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "-[id=42]-" ) );
    }

    @Test
    public void shouldHandleEdgeValue()
    {
        // Given
        NodeValue startNode = VirtualValues.nodeValue( 1L, Values.stringArray( "L" ), EMPTY_MAP );
        NodeValue endNode = VirtualValues.nodeValue( 2L, Values.stringArray( "L" ), EMPTY_MAP );
        EdgeValue edge = VirtualValues.edgeValue( 42L, startNode, endNode, stringValue( "R" ),
                props( "foo", intValue( 42 ), "bar", list( intValue( 1337 ), stringValue( "baz" ) ) ) );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        edge.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "-[id=42 :R {bar: [1337, \"baz\"], foo: 42}]-" ) );
    }

    @Test
    public void shouldHandleEdgeValueWithoutProperties()
    {
        NodeValue startNode = VirtualValues.nodeValue( 1L, Values.stringArray( "L" ), EMPTY_MAP );
        NodeValue endNode = VirtualValues.nodeValue( 2L, Values.stringArray( "L" ), EMPTY_MAP );
        EdgeValue edge = VirtualValues.edgeValue( 42L, startNode, endNode, stringValue( "R" ), EMPTY_MAP );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        edge.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "-[id=42 :R]-" ) );
    }

    @Test
    public void shouldHandleEdgeValueWithoutLabelsNorProperties()
    {
        // Given
        NodeValue node = VirtualValues.nodeValue( 42L, Values.stringArray(), EMPTY_MAP );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        node.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=42)" ) );
    }

    @Test
    public void shouldHandlePaths()
    {
        // Given
        NodeValue startNode = VirtualValues.nodeValue( 1L, Values.stringArray( "L" ), EMPTY_MAP );
        NodeValue endNode = VirtualValues.nodeValue( 2L, Values.stringArray( "L" ), EMPTY_MAP );
        EdgeValue edge = VirtualValues.edgeValue( 42L, startNode, endNode, stringValue( "R" ), EMPTY_MAP );
        PathValue path = VirtualValues.path( new NodeValue[]{startNode, endNode}, new EdgeValue[]{edge} );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        path.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "(id=1 :L)-[id=42 :R]->(id=2 :L)" ) );
    }

    @Test
    public void shouldHandleMaps()
    {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        MapValue mapValue = props( "k1", intValue( 42 ) );

        // When
        mapValue.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "{k1: 42}" ) );
    }

    @Test
    public void shouldHandleLists()
    {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        ListValue list = VirtualValues.list( stringValue( "foo" ), byteValue( (byte) 42 ) );

        // When
        list.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "[\"foo\", 42]" ) );
    }

    @Test
    public void shouldHandleArrays()
    {
        // Given
        PrettyPrinter printer = new PrettyPrinter();
        TextArray array = Values.stringArray( "a", "b", "c" );

        // When
        array.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "[\"a\", \"b\", \"c\"]" ) );
    }

    @Test
    public void shouldHandleBooleans()
    {
        // Given
        Value array = Values.booleanArray( new boolean[]{true, false, true} );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        array.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "[true, false, true]" ) );
    }

    @Test
    public void shouldHandleByteArrays()
    {
        // Given
        Value array = Values.byteArray( new byte[]{2, 3, 42} );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        array.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "[2, 3, 42]" ) );
    }

    @Test
    public void shouldHandleNull()
    {
        // Given
        PrettyPrinter printer = new PrettyPrinter();

        // When
        Values.NO_VALUE.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "<null>" ) );
    }

    @Test
    public void shouldHandlePoints()
    {
        // Given
        PointValue pointValue = VirtualValues.pointCartesian( 11d, 12d );
        PrettyPrinter printer = new PrettyPrinter();

        // When
        pointValue.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "{geometry: {type: \"Point\", coordinates: [11.0, 12.0], " +
                                              "crs: {type: link, properties: " +
                                              "{href: \"http://spatialreference.org/ref/sr-org/7203/\", code: " +
                                              "7203}}}}" ) );
    }

    @Test
    public void shouldBeAbleToUseAnyQuoteMark()
    {
        // Given
        TextValue hello = stringValue( "(ツ)" );
        PrettyPrinter printer = new PrettyPrinter( "__" );

        // When
        hello.writeTo( printer );

        // Then
        assertThat( printer.value(), equalTo( "__(ツ)__" ) );
    }

    private MapValue props( Object... keyValue )
    {
        HashMap<String,AnyValue> map = new HashMap<>( keyValue.length );
        String key = null;
        for ( int i = 0; i < keyValue.length; i++ )
        {
            if ( i % 2 == 0 )
            {
                key = (String) keyValue[i];
            }
            else
            {
                map.put( key, (AnyValue) keyValue[i] );
            }
        }
        return VirtualValues.map( map );
    }
}
