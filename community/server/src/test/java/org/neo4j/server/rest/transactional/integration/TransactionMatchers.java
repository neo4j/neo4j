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
package org.neo4j.server.rest.transactional.integration;

import java.text.ParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.util.RFC1123;
import org.neo4j.test.server.HTTP;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;

/**
 * Matchers and assertion methods for the transactional endpoint.
 */
public class TransactionMatchers
{
    static Matcher<String> isValidRFCTimestamp()
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            protected boolean matchesSafely( String item )
            {
                try
                {
                    return RFC1123.parseTimestamp( item ).getTime() > 0;
                }
                catch ( ParseException e )
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "valid RFC1134 timestamp" );
            }
        };
    }

    static Matcher<String> matches( final String pattern )
    {
        final Pattern regex = Pattern.compile( pattern );

        return new TypeSafeMatcher<String>()
        {
            @Override
            protected boolean matchesSafely( String item )
            {
                return regex.matcher( item ).matches();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "matching regex: " ).appendValue( pattern );
            }
        };
    }

    public static Matcher<? super HTTP.Response> containsNoErrors()
    {
        return hasErrors();
    }

    public static Matcher<? super HTTP.Response> hasErrors( final Status... expectedErrors )
    {
        return new TypeSafeMatcher<HTTP.Response>()
        {
            @Override
            protected boolean matchesSafely( HTTP.Response response )
            {
                try
                {
                    Iterator<JsonNode> errors = response.get( "errors" ).iterator();
                    Iterator<Status> expected = iterator( expectedErrors );

                    while ( expected.hasNext() )
                    {
                        assertTrue( errors.hasNext() );
                        assertThat( errors.next().get( "code" ).asText(), equalTo( expected.next().code().serialize() ) );
                    }
                    if ( errors.hasNext() )
                    {
                        JsonNode error = errors.next();
                        fail( "Expected no more errors, but got " + error.get( "code" ) + " - '" + error.get( "message" ) + "'." );
                    }
                    return true;

                }
                catch ( JsonParseException e )
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
            }
        };
    }

    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public static long countNodes(GraphDatabaseService graphdb)
    {
        try ( Transaction ignore = graphdb.beginTx() )
        {
            long count = 0;
            Iterator<Node> allNodes = GlobalGraphOperations.at( graphdb ).getAllNodes().iterator();
            while ( allNodes.hasNext() )
            {
                allNodes.next();
                count++;
            }
            return count;
        }
    }

    public static Matcher<? super HTTP.Response> containsNoStackTraces()
    {
        return new TypeSafeMatcher<HTTP.Response>()
        {
            @Override
            protected boolean matchesSafely( HTTP.Response response )
            {
                Map<String, Object> content = response.content();
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> errors = ((List<Map<String, Object>>) content.get( "errors" ));

                for ( Map<String, Object> error : errors )
                {
                    if( error.containsKey( "stackTrace" ) )
                    {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "contains stack traces" );
            }
        };
    }
}
