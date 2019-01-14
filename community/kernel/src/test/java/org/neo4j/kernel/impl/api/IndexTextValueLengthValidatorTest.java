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
package org.neo4j.kernel.impl.api;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.values.storable.Value;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

public class IndexTextValueLengthValidatorTest
{
    private static final int MAX_BYTE_LENGTH = 20_000;

    private IndexTextValueLengthValidator validator = new IndexTextValueLengthValidator( MAX_BYTE_LENGTH );

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void tooLongByteArrayIsNotAllowed()
    {
        int length = MAX_BYTE_LENGTH * 2;
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
        validator.validate( RandomUtils.nextBytes( length ) );
    }

    @Test
    public void tooLongStringIsNotAllowed()
    {
        int length = MAX_BYTE_LENGTH * 2;
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
        validator.validate( string( length ) );
    }

    @Test
    public void shortByteArrayIsValid()
    {
        validator.validate( RandomUtils.nextBytes( 3 ) );
        validator.validate( RandomUtils.nextBytes( 30 ) );
        validator.validate( RandomUtils.nextBytes( 300 ) );
        validator.validate( RandomUtils.nextBytes( 4303 ) );
        validator.validate( RandomUtils.nextBytes( 13234 ) );
        validator.validate( RandomUtils.nextBytes( MAX_BYTE_LENGTH ) );
    }

    @Test
    public void shortStringIsValid()
    {
        validator.validate( string( 3 ) );
        validator.validate( string( 30 ) );
        validator.validate( string( 300 ) );
        validator.validate( string( 4303 ) );
        validator.validate( string( 13234 ) );
        validator.validate( string( MAX_BYTE_LENGTH ) );
    }

    @Test
    public void nullIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Null value" );
        validator.validate( (byte[]) null );
    }

    @Test
    public void nullValueIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Null value" );
        validator.validate( (Value) null );
    }

    @Test
    public void noValueIsNotAllowed()
    {
        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Null value" );
        validator.validate( NO_VALUE );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldFailOnTooLongNonLatinString()
    {
        // given
        byte[] facesBytes = stringOfEmojis( 1 );

        // when
        validator.validate( stringValue( new String( facesBytes, UTF_8 ) ) );
    }

    @Test
    public void shouldSucceedOnReasonablyLongNonLatinString()
    {
        // given
        byte[] facesBytes = stringOfEmojis( 0 );

        // when
        validator.validate( stringValue( new String( facesBytes, UTF_8 ) ) );
    }

    private byte[] stringOfEmojis( int beyondMax )
    {
        byte[] poutingFaceSymbol = "\uD83D\uDE21".getBytes();
        int count = MAX_BYTE_LENGTH / poutingFaceSymbol.length + beyondMax;
        byte[] facesBytes = new byte[poutingFaceSymbol.length * count];
        for ( int i = 0; i < count; i++ )
        {
            System.arraycopy( poutingFaceSymbol, 0, facesBytes, i * poutingFaceSymbol.length, poutingFaceSymbol.length );
        }
        return facesBytes;
    }

    private Value string( int length )
    {
        return stringValue( RandomStringUtils.randomAlphabetic( length ) );
    }
}
