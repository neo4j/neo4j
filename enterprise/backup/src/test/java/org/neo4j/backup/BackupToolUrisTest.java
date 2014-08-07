/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.PrintStream;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.kernel.configuration.Config;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(Enclosed.class)
public class BackupToolUrisTest
{
    private abstract static class UriTests
    {
        final String host;
        final Integer port;
        final String uri;

        UriTests( String host, Integer port )
        {
            this.uri = (port == null) ? host : host + ":" + port;
            this.host = host.replace( "single://", "" ).replace( "ha://", "" );
            this.port = (port == null) ? BackupServer.DEFAULT_PORT : port;
        }

        static Object[] uri( String host )
        {
            return uri( host, null );
        }

        static Object[] uri( String host, Integer port )
        {
            return new Object[]{host, port};
        }

    }

    @RunWith(Parameterized.class)
    public static class ValidUriTests extends UriTests
    {
        public ValidUriTests( String host, Integer port )
        {
            super( host, port );
        }

        @Parameterized.Parameters
        public static List<Object[]> data()
        {
            return asList( uri( "127.0.0.1" ), uri( "127.0.0.1", 6362 ), uri( "localhost" ), uri( "localhost", 1234 ),
                    uri( "192.127.10.12" ), uri( "192.127.10.12", 20 ), uri( "1.1.1.1" ), uri( "1.1.1.1", 1 ),
                    uri( "google.company.com" ), uri( "google.company.com", 65200 ), uri( "single://localhost", 7090 ),
                    uri( "test-site.with-long.name.com", 55555 ), uri( "single://neo4j-backup.server", 6468 ),
                    uri( "single://apple.com" ), uri( "single://255.255.255.0" ), uri( "single://255.1.255.1", 88 ),
                    uri( "single://127.0.0.1" ), uri( "single://localhost" ), uri( "single://127.0.0.1", 6264 ) );
        }

        @Test
        public void shouldExecuteBackupWithValidUri() throws BackupTool.ToolFailureException
        {
            // given
            String[] args = new String[]{"-from", uri, "-to", "/var/backup/graph"};
            BackupService service = mock( BackupService.class );
            PrintStream systemOut = mock( PrintStream.class );

            // when
            new BackupTool( service, systemOut ).run( args );

            // then
            verify( service ).doIncrementalBackupOrFallbackToFull(
                    eq( host ),
                    eq( port ),
                    eq( "/var/backup/graph" ),
                    eq( true ),
                    any( Config.class )
            );
        }
    }

    @RunWith(Parameterized.class)
    public static class InvalidUriTests extends UriTests
    {
        public InvalidUriTests( String host, Integer port )
        {
            super( host, port );
        }

        @Parameterized.Parameters
        public static List<Object[]> data()
        {
            return asList( uri( "ha://1.1.1.1,ha://2.2.2.2" ), uri( "single://localhost,ha://not-localhost" ),
                    uri( "ha://*wrong,127.0.0.1:20" ), uri( "single://127.0.0.1:6361,single://127.0.0.1:6362" ),
                    uri( "300.400.500.600" ), uri( "ha://82.94.100.12:1212,127.0.0.*:1213" ),
                    uri( "single://127.0.0.1,ha://127.0.0.2" ), uri( "single:/\\/20.30.40.50:11,60.70.80.90:22" ),
                    uri( "host-name_with*wrong$chars.com" ) );
        }

        @Test
        public void shouldThrowForInvalidUri() throws BackupTool.ToolFailureException
        {
            // given
            String[] args = new String[]{"-from", uri, "-to", "/var/backup/graph"};
            BackupService service = mock( BackupService.class );
            PrintStream systemOut = mock( PrintStream.class );

            try
            {
                // when
                new BackupTool( service, systemOut ).run( args );
                fail( "Should exit abnormally" );
            }
            catch ( BackupTool.ToolFailureException e )
            {
                // then
                assertThat( e.getMessage(), equalTo( BackupTool.WRONG_URI_SYNTAX ) );
            }

            verifyZeroInteractions( service, systemOut );
        }
    }

    @RunWith(Parameterized.class)
    public static class InvalidSchemaTests extends UriTests
    {

        public InvalidSchemaTests( String host, Integer port )
        {
            super( host, port );
        }

        @Parameterized.Parameters
        public static List<Object[]> data()
        {
            return asList( uri( "dir://my" ), uri( "dir://my", 10 ),
                    uri( "foo://127.0.1.1" ), uri( "foo://127.0.1.1", 6567 ),
                    uri( "cat://localhost" ), uri( "cat://localhost", 4444 ) );
        }

        @Test
        public void shouldThrowForInvalidSchema() throws BackupTool.ToolFailureException
        {
            // given
            String[] args = new String[]{"-from", uri, "-to", "/var/backup"};
            BackupService service = mock( BackupService.class );
            PrintStream systemOut = mock( PrintStream.class );

            try
            {
                // when
                new BackupTool( service, systemOut ).run( args );
                fail( "Should exit abnormally" );
            }
            catch ( BackupTool.ToolFailureException e )
            {
                // then
                String schema = uri.substring( 0, uri.indexOf( ":" ) );
                assertThat( e.getMessage(), equalTo( format( BackupTool.UNKNOWN_SCHEMA_MESSAGE_PATTERN, schema ) ) );
            }

            verifyZeroInteractions( service, systemOut );
        }
    }
}
