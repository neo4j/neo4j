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
import org.junit.jupiter.api.Test;

import org.neo4j.values.storable.Value;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

class IndexTextValueLengthValidatorTest
{
    private static final int MAX_BYTE_LENGTH = 20_000;

    private IndexTextValueLengthValidator validator = new IndexTextValueLengthValidator( MAX_BYTE_LENGTH );

    @Test
    void tooLongByteArrayIsNotAllowed()
    {
        int length = MAX_BYTE_LENGTH * 2;
        var e = assertThrows( IllegalArgumentException.class, () -> validator.validate( RandomUtils.nextBytes( length ) ),
            "Property value is too large to index into this particular index. Please see index documentation for limitations." );
    }

    @Test
    void tooLongStringIsNotAllowed()
    {
        int length = MAX_BYTE_LENGTH * 2;
        var e = assertThrows( IllegalArgumentException.class, () -> validator.validate( string( length ) ),
            "Property value is too large to index into this particular index. Please see index documentation for limitations." );
    }

    @Test
    void shortByteArrayIsValid()
    {
        validator.validate( RandomUtils.nextBytes( 3 ) );
        validator.validate( RandomUtils.nextBytes( 30 ) );
        validator.validate( RandomUtils.nextBytes( 300 ) );
        validator.validate( RandomUtils.nextBytes( 4303 ) );
        validator.validate( RandomUtils.nextBytes( 13234 ) );
        validator.validate( RandomUtils.nextBytes( MAX_BYTE_LENGTH ) );
    }

    @Test
    void shortStringIsValid()
    {
        validator.validate( string( 3 ) );
        validator.validate( string( 30 ) );
        validator.validate( string( 300 ) );
        validator.validate( string( 4303 ) );
        validator.validate( string( 13234 ) );
        validator.validate( string( MAX_BYTE_LENGTH ) );
    }

    @Test
    void nullIsNotAllowed()
    {
        assertThrows(IllegalArgumentException.class, () -> validator.validate( (byte[]) null ), "Null value");
    }

    @Test
    void nullValueIsNotAllowed()
    {
        assertThrows(IllegalArgumentException.class, () -> validator.validate( (Value) null ), "Null value");
    }

    @Test
    void noValueIsNotAllowed()
    {
        assertThrows(IllegalArgumentException.class, () -> validator.validate( NO_VALUE ), "Null value");
    }

    @Test
    void shouldFailOnTooLongNonLatinString()
    {
        // given
        byte[] facesBytes = stringOfEmojis( 1 );

        // when
        assertThrows( IllegalArgumentException.class, () -> validator.validate( stringValue( new String( facesBytes, UTF_8 ) ) ) );
    }

    @Test
    void shouldSucceedOnReasonablyLongNonLatinString()
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
