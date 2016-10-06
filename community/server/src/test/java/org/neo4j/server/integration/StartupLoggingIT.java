/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ClientConnectorSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;

public class StartupLoggingIT extends ExclusiveServerTestBase
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Before
    public void setUp() throws IOException
    {
        FileUtils.deleteRecursively( ServerTestUtils.getRelativeFile( DatabaseManagementSystemSettings.data_directory ) );
    }

    @Rule
    public TestDirectory homeDir = TestDirectory.testDirectory();

    @Test
    public void shouldLogHelpfulStartupMessages() throws Throwable
    {
        CommunityBootstrapper boot = new CommunityBootstrapper();
        Pair[] propertyPairs = getPropertyPairs();

        boot.start( homeDir.directory(), Optional.of( new File( "nonexistent-file.conf" ) ), propertyPairs );
        boot.stop();

        List<String> captured = suppressOutput.getOutputVoice().lines();
        assertThat( captured, containsAtLeastTheseLines(
                warn( "Config file \\[nonexistent-file.conf\\] does not exist." ),
                info( "Starting..." ),
                info( "Started." ),
                info( "Remote interface available at http://.+:7474/" ),
                info( "Stopping..." ),
                info( "Stopped." )
        ) );
    }

    private Pair[] getPropertyPairs() throws IOException
    {
        List<Pair> pairs = new ArrayList<>();
        Map<String,String> relativeProperties = ServerTestUtils.getDefaultRelativeProperties();
        for ( Map.Entry<String,String> entry : relativeProperties.entrySet() )
        {
            pairs.add( Pair.of( entry.getKey(), entry.getValue() ) );
        }
        pairs.add( Pair.of( GraphDatabaseSettings.allow_store_upgrade.name(), Settings.TRUE) );
        pairs.add( Pair.of( ClientConnectorSettings.httpConnector("http").type.name(), "HTTP" ) );
        pairs.add( Pair.of( ClientConnectorSettings.httpConnector("http").enabled.name(), Settings.TRUE ) );
        return pairs.toArray( new Pair[pairs.size()] );
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
