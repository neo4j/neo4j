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
package org.neo4j.values.storable;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.utf8Value;

@RunWith( Parameterized.class )
public class TextValueTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameterized.Parameter
    public Function<String,TextValue> value;

    @Parameterized.Parameters
    public static Collection<Function<String,TextValue>> functions()
    {
        return asList( Values::stringValue, s -> utf8Value( s.getBytes( StandardCharsets.UTF_8 ) ) );
    }

    @Test
    public void replace()
    {
        assertThat( value.apply( "hello" ).replace( "l", "w" ), equalTo( value.apply( "hewwo" ) ) );
        assertThat( value.apply( "hello" ).replace( "ell", "ipp" ), equalTo( value.apply( "hippo" ) ) );
        assertThat( value.apply( "hello" ).replace( "a", "x" ), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "hello" ).replace( "e", "" ), equalTo( value.apply( "hllo" ) ) );
        assertThat( value.apply( "" ).replace( "", "⁻" ), equalTo( value.apply( "⁻" ) ) );
    }

    @Test
    public void substring()
    {
        assertThat( value.apply( "hello" ).substring( 2, 5 ), equalTo( value.apply( "llo" ) ) );
        assertThat( value.apply( "hello" ).substring( 4, 5 ), equalTo( value.apply( "o" ) ) );
        assertThat( value.apply( "hello" ).substring( 1, 3 ), equalTo( value.apply( "ell" ) ) );
        assertThat( value.apply( "hello" ).substring( 8, 5 ), equalTo( StringValue.EMTPY ) );
        assertThat( value.apply( "0123456789" ).substring( 1 ), equalTo( value.apply( "123456789" ) ) );
        assertThat( value.apply( "0123456789" ).substring( 5 ), equalTo( value.apply( "56789" ) ) );
        assertThat( value.apply( "0123456789" ).substring( 15 ), equalTo( StringValue.EMTPY ) );
        assertThat( value.apply( "\uD83D\uDE21\uD83D\uDCA9\uD83D\uDC7B" ).substring( 1, 1 ),
                equalTo( value.apply( "\uD83D\uDCA9" ) ) );
        assertThat( value.apply( "\uD83D\uDE21\uD83D\uDCA9\uD83D\uDC7B" ).substring( 1, 2 ),
                equalTo( value.apply( "\uD83D\uDCA9\uD83D\uDC7B" ) ) );

        exception.expect( IndexOutOfBoundsException.class );
        value.apply( "hello" ).substring( -4, 2 );
    }

    @Test
    public void toLower()
    {
        assertThat( value.apply( "HELLO" ).toLower(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "Hello" ).toLower(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "hello" ).toLower(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "" ).toLower(), equalTo( value.apply( "" ) ) );
    }

    @Test
    public void toUpper()
    {
        assertThat( value.apply( "HELLO" ).toUpper(), equalTo( value.apply( "HELLO" ) ) );
        assertThat( value.apply( "Hello" ).toUpper(), equalTo( value.apply( "HELLO" ) ) );
        assertThat( value.apply( "hello" ).toUpper(), equalTo( value.apply( "HELLO" ) ) );
        assertThat( value.apply( "" ).toUpper(), equalTo( value.apply( "" ) ) );
    }

    @Test
    public void ltrim()
    {
        assertThat( value.apply( "  HELLO" ).ltrim(), equalTo( value.apply( "HELLO" ) ) );
        assertThat( value.apply( " Hello" ).ltrim(), equalTo( value.apply( "Hello" ) ) );
        assertThat( value.apply( "  hello  " ).ltrim(), equalTo( value.apply( "hello  " ) ) );
        assertThat( value.apply( "\u2009㺂࿝鋦毠\u2009" ).ltrim(), equalTo( value.apply( "㺂࿝鋦毠\u2009" ) ) );
    }

    @Test
    public void rtrim()
    {
        assertThat( value.apply( "HELLO  " ).rtrim(), equalTo( value.apply( "HELLO" ) ) );
        assertThat( value.apply( "Hello  " ).rtrim(), equalTo( value.apply( "Hello" ) ) );
        assertThat( value.apply( "  hello  " ).rtrim(), equalTo( value.apply( "  hello" ) ) );
        assertThat( value.apply( "\u2009㺂࿝鋦毠\u2009" ).rtrim(), equalTo( value.apply( "\u2009㺂࿝鋦毠" ) ) );
    }

    @Test
    public void trim()
    {
        assertThat( value.apply( "  hello  " ).trim(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "  hello " ).trim(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "hello " ).trim(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "  hello" ).trim(), equalTo( value.apply( "hello" ) ) );
        assertThat( value.apply( "\u2009㺂࿝鋦毠\u2009" ).trim(), equalTo( value.apply( "㺂࿝鋦毠" ) ) );
    }

    @Test
    public void reverse()
    {
        assertThat( value.apply( "Foo" ).reverse(), equalTo( value.apply( "ooF" ) ) );
        assertThat( value.apply( "" ).reverse(), equalTo( StringValue.EMTPY ) );
        assertThat( value.apply( " L" ).reverse(), equalTo( value.apply( "L " ) ) );
        assertThat( value.apply( "\r\n" ).reverse(), equalTo( value.apply( "\n\r" ) ) );
        assertThat( value.apply( "\uD801\uDC37" ).reverse(), equalTo( value.apply( "\uD801\uDC37" ) ) );
        assertThat( value.apply( "This is literally a pile of crap \uD83D\uDCA9, it is fantastic" ).reverse(),
                equalTo( value.apply( "citsatnaf si ti ,\uD83D\uDCA9 parc fo elip a yllaretil si sihT" ) ) );
        assertThat( value.apply( "\uD83D\uDE21\uD83D\uDCA9\uD83D\uDC7B" ).reverse(), equalTo( value.apply(
                "\uD83D\uDC7B\uD83D\uDCA9\uD83D\uDE21" ) ) );
    }

    @Test
    public void split()
    {
        assertThat( value.apply( "HELLO" ).split( "LL" ), equalTo( stringArray( "HE", "O" ) ) );
        assertThat( value.apply( "Separating,by,comma,is,a,common,use,case" ).split( "," ),
                equalTo( stringArray( "Separating", "by", "comma", "is", "a", "common", "use", "case" ) ) );
        assertThat( value.apply( "HELLO" ).split( "HELLO" ), equalTo( stringArray( "", "" ) ) );

    }
}
