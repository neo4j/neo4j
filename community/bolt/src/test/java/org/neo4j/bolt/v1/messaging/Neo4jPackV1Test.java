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
package org.neo4j.bolt.v1.messaging;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.bolt.v1.runtime.Neo4jError;
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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.bolt.v1.messaging.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Paths.ALL_PATHS;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.charValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

@EnableRuleMigrationSupport
public class Neo4jPackV1Test
{
    private final Neo4jPackV1 neo4jPack = new Neo4jPackV1();

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
    public void shouldBeAbleToPackAndUnpackList() throws IOException
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
    public void shouldBeAbleToPackAndUnpackMap() throws IOException
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
    public void shouldFailWhenTryingToPackAndUnpackMapContainingNullKeys() throws IOException
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
        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
        unpacker.unpack();

        // Then
        assertThat( unpacker.consumeError(), equalTo(
                Neo4jError.from( Status.Request.Invalid,
                        "Value `null` is not supported as key in maps, must be a non-nullable string." ) ) );
    }

    @Test
    public void shouldErrorOnUnpackingMapWithDuplicateKeys() throws IOException
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
        PackedInputArray input = new PackedInputArray( output.bytes() );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );
        unpacker.unpack();

        // Then
        assertThat( unpacker.consumeError(),
                equalTo( Neo4jError.from( Status.Request.Invalid, "Duplicate map key `key`." ) ) );
    }

    @Test
    public void shouldNotBeAbleToUnpackNode() throws IOException
    {
        // Expect
        exception.expect( BoltIOException.class );
        // When
        unpacked( packed( ALICE ) );
    }

    @Test
    public void shouldNotBeAbleToUnpackRelationship() throws IOException
    {
        // Expect
        exception.expect( BoltIOException.class );
        // When
        unpacked( packed( ALICE_KNOWS_BOB ) );
    }

    @Test
    public void shouldNotBeAbleToUnpackPaths() throws IOException
    {
        for ( PathValue path : ALL_PATHS )
        {
            // Expect
            exception.expect( BoltIOException.class );
            // When
            unpacked( packed( path ) );
        }
    }

    @Test
    public void shouldTreatSingleCharAsSingleCharacterString() throws IOException
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
    public void shouldTreatCharArrayAsListOfStrings() throws IOException
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
    public void shouldPackUtf8() throws IOException
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
