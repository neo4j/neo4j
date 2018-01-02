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
package org.neo4j.server.plugins;

import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;

import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class PluginFunctionalTestHelper
{
    public static Map<String, Object> makeGet( String url ) throws JsonParseException {
        JaxRsResponse response = new RestRequest().get(url);

        String body = getResponseText(response);
        response.close();

        return deserializeMap(body);
    }

    protected static Map<String, Object> deserializeMap( final String body ) throws JsonParseException
    {
        Map<String, Object> result = JsonHelper.jsonToMap( body );
        assertThat( result, CoreMatchers.is( not( nullValue() ) ) );
        return result;
    }

    private static List<Map<String, Object>> deserializeList( final String body ) throws JsonParseException
    {
        List<Map<String, Object>> result = JsonHelper.jsonToList( body );
        assertThat( result, CoreMatchers.is( not( nullValue() ) ) );
        return result;
    }

    protected static String getResponseText( final JaxRsResponse response )
    {
        String body = response.getEntity();

        Assert.assertEquals( body, 200, response.getStatus() );
        return body;
    }

    protected static Map<String, Object> makePostMap( String url ) throws JsonParseException
    {
        JaxRsResponse response = new RestRequest().post(url,null);

        String body = getResponseText( response );
        response.close();

        return deserializeMap( body );
    }

    protected static Map<String, Object> makePostMap( String url, Map<String, Object> params )
            throws JsonParseException
    {
        String json = JsonHelper.createJsonFrom( params );
        JaxRsResponse response = new RestRequest().post(url, json, MediaType.APPLICATION_JSON_TYPE);

        String body = getResponseText( response );
        response.close();

        return deserializeMap( body );
    }

    protected static List<Map<String, Object>> makePostList( String url ) throws JsonParseException {
        JaxRsResponse response = new RestRequest().post(url, null);

        String body = getResponseText(response);
        response.close();

        return deserializeList(body);
    }

    protected static List<Map<String, Object>> makePostList( String url, Map<String, Object> params )
            throws JsonParseException {
        String json = JsonHelper.createJsonFrom(params);
        JaxRsResponse response = new RestRequest().post(url, json);

        String body = getResponseText(response);
        response.close();

        return deserializeList(body);
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
