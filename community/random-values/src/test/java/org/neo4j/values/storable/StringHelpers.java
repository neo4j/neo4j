/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.values.storable;

import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

final class StringHelpers
{
    private StringHelpers()
    {
        throw new UnsupportedOperationException();
    }

    static <T> void assertConsistent( String string, Function<TextValue,T> test )
    {
        TextValue textValue = stringValue( string );
        TextValue utf8Value = utf8Value( string.getBytes( UTF_8 ) );
        T a = test.apply( textValue );
        T b = test.apply( utf8Value );

        String errorMsg = format( "operation not consistent for %s", string );
        assertThat( errorMsg, a, equalTo( b ) );
        assertThat( errorMsg, b, equalTo( a ) );
    }

    static <T> void assertConsistent( String string1, String string2, BiFunction<TextValue,TextValue,T> test )
    {
        TextValue textValue1 = stringValue( string1 );
        TextValue textValue2 = stringValue( string2 );
        TextValue utf8Value1 = utf8Value( string1.getBytes( UTF_8 ) );
        TextValue utf8Value2 = utf8Value( string2.getBytes( UTF_8 ) );
        T a = test.apply( textValue1, textValue2 );
        T x = test.apply( textValue1, utf8Value2 );
        T y = test.apply( utf8Value1, textValue2 );
        T z = test.apply( utf8Value1, utf8Value2 );

        String errorMsg = format( "operation not consistent for `%s` and `%s`", string1, string2 );
        assertThat( errorMsg, a, equalTo( x ) );
        assertThat( errorMsg, x, equalTo( a ) );
        assertThat( errorMsg, a, equalTo( y ) );
        assertThat( errorMsg, y, equalTo( a ) );
        assertThat( errorMsg, a, equalTo( z ) );
        assertThat( errorMsg, z, equalTo( a ) );
    }
}
