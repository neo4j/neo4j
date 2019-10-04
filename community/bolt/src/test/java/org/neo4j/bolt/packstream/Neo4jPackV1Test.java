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
package org.neo4j.bolt.packstream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UTF8StringValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.bolt.packstream.Neo4jPackV1.UNBOUND_RELATIONSHIP;
import static org.neo4j.bolt.packstream.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.packstream.example.Nodes.ALICE;
import static org.neo4j.bolt.packstream.example.Paths.ALL_PATHS;
import static org.neo4j.kernel.api.exceptions.Status.Statement.TypeError;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

public class Neo4jPackV1Test
{
    private final Neo4jPackV1 neo4jPack = new Neo4jPackV1();

    private byte[] packed( AnyValue object ) throws IOException
    {
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.pack( object );
        return output.bytes();
    }

    private AnyValue unpacked( byte[] bytes ) throws IOException
    {
        PackedInputArray input = new PackedInputArray( bytes );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
        return unpacker.unpack();
    }

    @Test
    void shouldBeAbleToPackAndUnpackList() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.packListHeader( ALICE.labels().length() );
        List<String> expected = new ArrayList<>();
        TextArray labels = ALICE.labels();
        for ( int i = 0; i < labels.length(); i++ )
        {
            String labelName = labels.stringValue( i );
            packer.pack( labelName );
            expected.add( labelName );
        }
        AnyValue unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( ListValue.class ) );
        ListValue unpackedList = (ListValue) unpacked;
        assertThat( unpackedList, equalTo( ValueUtils.asListValue( expected ) ) );
    }

    @Test
    void shouldBeAbleToPackAndUnpackMap() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.packMapHeader( ALICE.properties().size() );
        ALICE.properties().foreach( ( s, value ) ->
        {
            try
            {
                packer.pack( s );
                packer.pack( value );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
        AnyValue unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( MapValue.class ) );
        MapValue unpackedMap = (MapValue) unpacked;
        assertThat( unpackedMap, equalTo( ALICE.properties() ) );
    }

    @Test
    void shouldFailWhenTryingToPackAndUnpackMapContainingNullKeys() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );

        Map<String,AnyValue> map = new HashMap<>();
        map.put( null, longValue( 42L ) );
        map.put( "foo", longValue( 1337L ) );
        packer.packMapHeader( map.size() );
        for ( Map.Entry<String,AnyValue> entry : map.entrySet() )
        {
            packer.pack( entry.getKey() );
            packer.pack( entry.getValue() );
        }

        // When
        try
        {
            PackedInputArray input = new PackedInputArray( output.bytes() );
            Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
            unpacker.unpack();

            fail( "exception expected" );
        }
        catch ( BoltIOException ex )
        {
            assertEquals( Neo4jError.from( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." ),
                    Neo4jError.from( ex ) );
        }
    }

    @Test
    void shouldThrowOnUnpackingMapWithDuplicateKeys() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.packMapHeader( 2 );
        packer.pack( "key" );
        packer.pack( intValue( 1 ) );
        packer.pack( "key" );
        packer.pack( intValue( 2 ) );

        // When
        try
        {
            PackedInputArray input = new PackedInputArray( output.bytes() );
            Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
            unpacker.unpack();

            fail( "exception expected" );
        }
        catch ( BoltIOException ex )
        {
            assertEquals( Neo4jError.from( Status.Request.Invalid, "Duplicate map key `key`." ), Neo4jError.from( ex ) );
        }
    }

    @Test
    void shouldThrowOnUnpackingMapWithUnsupportedKeyType() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.packMapHeader( 2 );
        packer.pack( ValueUtils.of( 1L ) );
        packer.pack( intValue( 1 ) );

        // When
        try
        {
            PackedInputArray input = new PackedInputArray( output.bytes() );
            Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
            unpacker.unpack();

            fail( "exception expected" );
        }
        catch ( BoltIOException ex )
        {
            assertEquals( Neo4jError.from( Status.Request.InvalidFormat, "Bad key type: INTEGER" ), Neo4jError.from( ex ) );
        }
    }

    @Test
    void shouldNotBeAbleToUnpackNode() throws IOException
    {
        var ex = assertThrows( BoltIOException.class, () -> unpacked( packed( ALICE ) ) );
        assertEquals( Neo4jError.from( TypeError, "Node values cannot be unpacked with this version of bolt." ), Neo4jError.from( ex ) );
    }

    @Test
    void shouldNotBeAbleToUnpackRelationship() throws IOException
    {
        var ex = assertThrows( BoltIOException.class, () -> unpacked( packed( ALICE_KNOWS_BOB ) ) );
        assertEquals( Neo4jError.from( TypeError, "Relationship values cannot be unpacked with this version of bolt." ),
                    Neo4jError.from( ex ) );
    }

    @Test
    void shouldNotBeAbleToUnpackUnboundRelationship() throws IOException
    {
        // Given
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPackV1.Packer packer = neo4jPack.newPacker( out );

        packer.packStructHeader( 3, UNBOUND_RELATIONSHIP );
        packer.pack( ValueUtils.of( 1L ) );
        packer.pack( ValueUtils.of( "RELATES_TO" ) );
        packer.pack( ValueUtils.asMapValue( MapUtil.map( "a", 1L, "b", "x" ) ) );

        var ex = assertThrows( BoltIOException.class, () -> unpacked( out.bytes() ) );
        assertEquals( Neo4jError.from( TypeError, "Relationship values cannot be unpacked with this version of bolt." ),
                    Neo4jError.from( ex ) );
    }

    @Test
    void shouldNotBeAbleToUnpackPaths() throws IOException
    {
        for ( PathValue path : ALL_PATHS )
        {
            var ex = assertThrows( BoltIOException.class, () -> unpacked( packed( path ) ) );
            assertEquals( Neo4jError.from( TypeError, "Path values cannot be unpacked with this version of bolt." ),
                        Neo4jError.from( ex ) );
        }
    }

    @Test
    void shouldTreatSingleCharAsSingleCharacterString() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.pack( charValue( 'C' ) );
        AnyValue unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( TextValue.class ) );
        assertThat( unpacked, equalTo( stringValue( "C" ) ) );
    }

    @Test
    void shouldTreatCharArrayAsListOfStrings() throws IOException
    {
        // Given
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.pack( charArray( new char[]{'W', 'H', 'Y'} ) );
        Object unpacked = unpacked( output.bytes() );

        // Then
        assertThat( unpacked, instanceOf( ListValue.class ) );
        assertThat( unpacked,
                equalTo( VirtualValues.list( stringValue( "W" ), stringValue( "H" ), stringValue( "Y" ) ) ) );
    }

    @Test
    void shouldPackUtf8() throws IOException
    {
        // Given
        String value = "\uD83D\uDE31";
        byte[] bytes = value.getBytes( StandardCharsets.UTF_8 );
        TextValue textValue = utf8Value( bytes, 0, bytes.length );
        PackedOutputArray output = new PackedOutputArray();
        Neo4jPack.Packer packer = neo4jPack.newPacker( output );
        packer.pack( textValue );

        // When
        AnyValue unpacked = unpacked( output.bytes() );
        assertThat( unpacked, is( instanceOf( UTF8StringValue.class ) ));

        // Then
        assertThat( unpacked, equalTo( textValue ) );
    }
}
