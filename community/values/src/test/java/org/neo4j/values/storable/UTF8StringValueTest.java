package org.neo4j.values.storable;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

public class UTF8StringValueTest
{
    private String[] strings = {"", "1337", " ", "普通话/普通話", "\uD83D\uDE21"};

    @Test
    public void shouldHandleDifferentTypesOfStrings()
    {
        for ( String string : strings )
        {
            TextValue stringValue = stringValue( string );
            byte[] bytes = string.getBytes( StandardCharsets.UTF_8 );
            TextValue utf8 = utf8Value( bytes, 0, bytes.length );
            assertSame( stringValue, utf8 );
        }
    }

    @Test
    public void shouldHandleOffset()
    {
        // Given
        byte[] bytes = "abcdefg".getBytes( StandardCharsets.UTF_8 );

        // When
        TextValue textValue = utf8Value( bytes, 3, 2 );

        // Then
        assertSame( textValue, stringValue( "de" ) );
    }

    private void assertSame( TextValue lhs, TextValue rhs )
    {
        assertThat( lhs.length(), equalTo( rhs.length() ) );
        assertThat( lhs, equalTo( rhs ) );
        assertThat( rhs, equalTo( lhs ) );
        assertThat( lhs.hashCode(), equalTo( rhs.hashCode() ) );
    }
}