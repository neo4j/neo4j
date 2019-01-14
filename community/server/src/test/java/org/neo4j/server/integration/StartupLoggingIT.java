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
package org.neo4j.server.integration;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.server.AbstractNeoServer.NEO4J_IS_STARTING_MESSAGE;

public class StartupLoggingIT extends ExclusiveServerTestBase
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Test
    public void shouldLogHelpfulStartupMessages()
    {
        CommunityBootstrapper boot = new CommunityBootstrapper();
        Map<String,String> propertyPairs = getPropertyPairs();

        boot.start( testDir.directory(), Optional.of( new File( "nonexistent-file.conf" ) ), propertyPairs );
        URI uri = boot.getServer().baseUri();
        boot.stop();

        List<String> captured = suppressOutput.getOutputVoice().lines();
        assertThat( captured, containsAtLeastTheseLines(
                warn( "Config file \\[nonexistent-file.conf\\] does not exist." ),
                info( NEO4J_IS_STARTING_MESSAGE ),
                info( "Starting..." ),
                info( "Started." ),
                info( "Remote interface available at " + uri.toString() ),
                info( "Stopping..." ),
                info( "Stopped." )
        ) );
    }

    private Map<String,String> getPropertyPairs()
    {
        Map<String,String> properties = new HashMap<>();

        properties.put( GraphDatabaseSettings.data_directory.name(), testDir.graphDbDir().toString() );
        properties.put( GraphDatabaseSettings.logs_directory.name(), testDir.graphDbDir().toString() );
        properties.put( LegacySslPolicyConfig.certificates_directory.name(), testDir.graphDbDir().toString() );
        properties.put( GraphDatabaseSettings.allow_upgrade.name(), Settings.TRUE );
        properties.put( ServerSettings.script_enabled.name(), Settings.TRUE );

        HttpConnector http = new HttpConnector( "http", Encryption.NONE );
        properties.put( http.type.name(), "HTTP" );
        properties.put( http.listen_address.name(), "localhost:0" );
        properties.put( http.enabled.name(), Settings.TRUE );

        HttpConnector https = new HttpConnector( "https", Encryption.TLS );
        properties.put( https.type.name(), "HTTP" );
        properties.put( https.listen_address.name(), "localhost:0" );
        properties.put( https.enabled.name(), Settings.TRUE );

        BoltConnector bolt = new BoltConnector( DEFAULT_CONNECTOR_KEY );
        properties.put( bolt.type.name(), "BOLT" );
        properties.put( bolt.enabled.name(), "true" );
        properties.put( bolt.listen_address.name(), "localhost:0" );

        properties.put( GraphDatabaseSettings.database_path.name(), testDir.absolutePath().getAbsolutePath() );
        return properties;
    }

    @SafeVarargs
    private static Matcher<List<String>> containsAtLeastTheseLines( final Matcher<String>... expectedLinePatterns )
    {
        return new TypeSafeMatcher<List<String>>()
        {
            @Override
            protected boolean matchesSafely( List<String> lines )
            {
                if ( expectedLinePatterns.length > lines.size() )
                {
                    return false;
                }

                for ( int i = 0, e = 0; i < lines.size(); i++ )
                {
                    String line = lines.get( i );
                    while ( !expectedLinePatterns[e].matches( line ) )
                    {
                        if ( ++i >= lines.size() )
                        {
                            return false;
                        }
                        line = lines.get( i );
                    }
                    e++;

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

    public static Matcher<String> info( String messagePattern )
    {
        return line( "INFO", messagePattern );
    }

    public static Matcher<String> warn( String messagePattern )
    {
        return line( "WARN", messagePattern );
    }

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
