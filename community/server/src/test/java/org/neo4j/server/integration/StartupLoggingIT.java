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
package org.neo4j.server.integration;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Arrays.asList;

public class StartupLoggingIT extends ExclusiveServerTestBase
{
    @Test
    public void shouldLogHelpfulStartupMessages() throws Throwable
    {
        // Given
        SuppressOutput suppressed = SuppressOutput.suppressAll();

        // When
        suppressed.call( new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                CommunityBootstrapper boot = new CommunityBootstrapper();
                CommunityBootstrapper.start( boot, new String[]{} );
                boot.stop();
                return null;
            }
        });

        // Then
        List<String> captured = suppressed.getOutputVoice().lines();
        // TODO: Obviously the logging below is insane, but we added this test in a point release, so we don't want to break anyone grepping for this
        //       This should be changed in 3.0.0.
        assertThat( captured, containsAtLeastTheseLines(
                warn( "Config file \\[config.neo4j-server\\.properties\\] does not exist." ),
                warn( "Config file \\[config.neo4j\\.properties\\] does not exist." ),
                info( "Successfully started database" ),
                info( "Starting HTTP on port 7474 \\(.+ threads available\\)" ),
                info( "Mounting static content at /webadmin" ),
                info( "Mounting static content at /browser" ),
                info( "Remote interface ready and available at http://.+:7474/" ),

                info( "Successfully shutdown Neo4j Server" ),
                info( "Successfully stopped database" ),
                info( "Successfully shutdown database" ),
                info( "Successfully shutdown Neo Server on port \\[.+\\], database \\[.+\\]")
        ) );
    }

    public static Matcher<List<String>> containsAtLeastTheseLines( final Matcher<String> ... expectedLinePatterns )
    {
        return new TypeSafeMatcher<List<String>>()
        {
            @Override
            protected boolean matchesSafely( List<String> lines )
            {
                if(expectedLinePatterns.length > lines.size())
                {
                    return false;
                }

                for ( int i = 0, e = 0; i < lines.size(); i++ )
                {
                    String line = lines.get( i );
                    boolean matches;
                    while ( (matches = expectedLinePatterns[e].matches( line )) == false )
                    {
                        if ( ++i >= lines.size() )
                        {
                            return false;
                        }
                        line = lines.get( i );
                    }
                    e++;

                    if ( !matches )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendList( "", "\n", "", asList(expectedLinePatterns) );
            }
        };
    }

    public static Matcher<String> info( String messagePattern ) { return line("INFO", messagePattern); }
    public static Matcher<String> warn( String messagePattern ) { return line("WARN", messagePattern); }

    public static Matcher<String> line( final String level, final String messagePattern )
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            protected boolean matchesSafely( String line )
            {
                return line.matches( ".*" + level + "\\s+" + messagePattern );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( level ).appendText( " " ).appendText( messagePattern );
            }
        };
    }
}
