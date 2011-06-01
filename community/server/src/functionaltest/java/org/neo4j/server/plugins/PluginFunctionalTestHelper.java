package org.neo4j.server.plugins;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.WebTestUtils.CLIENT;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import com.sun.jersey.api.client.ClientResponse;

public class PluginFunctionalTestHelper
{
    public static Map<String, Object> makeGet( String url ) throws JsonParseException
    {
        ClientResponse response = CLIENT.resource( url )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );

        String body = getResponseText( response );
        response.close();

        return deserializeMap( body );
    }

    protected static Map<String, Object> deserializeMap( final String body ) throws JsonParseException
    {
        Map<String, Object> result = JsonHelper.jsonToMap( body );
        assertThat( result, is( not( nullValue() ) ) );
        return result;
    }

    private static List<Map<String, Object>> deserializeList( final String body ) throws JsonParseException
    {
        List<Map<String, Object>> result = JsonHelper.jsonToList( body );
        assertThat( result, is( not( nullValue() ) ) );
        return result;
    }

    protected static String getResponseText( final ClientResponse response )
    {
        String body = response.getEntity( String.class );

        assertEquals( body, 200, response.getStatus() );
        return body;
    }

    protected static Map<String, Object> makePostMap( String url ) throws JsonParseException
    {
        ClientResponse response = CLIENT.resource( url )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class );

        String body = getResponseText( response );
        response.close();

        return deserializeMap( body );
    }

    protected static Map<String, Object> makePostMap( String url, Map<String, Object> params ) throws JsonParseException
    {
        String json = JsonHelper.createJsonFrom( params );
        ClientResponse response = CLIENT.resource( url )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( json, MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class );

        String body = getResponseText( response );
        response.close();

        return deserializeMap( body );
    }

    protected static List<Map<String, Object>> makePostList( String url ) throws JsonParseException
    {
        ClientResponse response = CLIENT.resource( url )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class );

        String body = getResponseText( response );
        response.close();

        return deserializeList( body );
    }

    protected static List<Map<String, Object>> makePostList( String url, Map<String, Object> params )
            throws JsonParseException
    {
        String json = JsonHelper.createJsonFrom( params );
        ClientResponse response = CLIENT.resource( url )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .entity( json, MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class );

        String body = getResponseText( response );
        response.close();

        return deserializeList( body );
    }

    public static class RegExp extends TypeSafeMatcher<String>
    {
        enum MatchType
        {
            end( "ends with" )
            {
                @Override
                boolean match( String pattern, String string )
                {
                    return string.endsWith( pattern );
                }
            },
            matches()
            {
                @Override
                boolean match( String pattern, String string )
                {
                    return string.matches( pattern );
                }
            },
            ;
            private final String description;

            abstract boolean match( String pattern, String string );

            private MatchType()
            {
                this.description = name();
            }

            private MatchType( String description )
            {
                this.description = description;
            }
        }

        private final String pattern;
        private String string;
        private final MatchType type;

        RegExp( String regexp, MatchType type )
        {
            this.pattern = regexp;
            this.type = type;
        }

        @Factory
        public static Matcher<String> endsWith( String pattern )
        {
            return new RegExp( pattern, MatchType.end );
        }

        @Override
        public boolean matchesSafely( String string )
        {
            this.string = string;
            return type.match( pattern, string );
        }

        @Override
        public void describeTo( Description descr )
        {
            descr.appendText( "expected something that " )
                    .appendText( type.description )
                    .appendText( " [" )
                    .appendText( pattern )
                    .appendText( "] but got [" )
                    .appendText( string )
                    .appendText( "]" );
        }
    }

}
