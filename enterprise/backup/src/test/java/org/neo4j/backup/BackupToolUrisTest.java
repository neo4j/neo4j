/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;


@RunWith(Enclosed.class)
public class BackupToolUrisTest
{
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
            return asList(
                    uri( "127.0.0.1" ),
                    uri( "127.0.0.1", 6362 ),
                    uri( "localhost" ),
                    uri( "localhost", 1234 ),
                    uri( "192.127.10.12" ),
                    uri( "192.127.10.12", 20 ),
                    uri( "1.1.1.1" ),
                    uri( "1.1.1.1", 1 ),
                    uri( "neo4j.company.com" ),
                    uri( "neo4j.company.com", 65200 ),
                    uri( "single://localhost", 7090 ),
                    uri( "test-site.with-long.name.com", 55555 ),
                    uri( "single://neo4j-backup.server", 6468 ),
                    uri( "single://apple.com" ),
                    uri( "single://255.255.255.0" ),
                    uri( "single://255.1.255.1", 88 ),
                    uri( "single://127.0.0.1" ),
                    uri( "single://localhost" ),
                    uri( "single://127.0.0.1", 6264 ),
                    uri( "ha://test.server" ),
                    uri( "ha://test.server", 1212 )
            );
        }

        @Test
        public void shouldExecuteBackupWithValidUri() throws Exception
        {
            // Given
            String[] args = new String[]{"-from", uri, "-to", "/var/backup/graph"};

            // When
            newBackupTool().run( args );

            // Then
            verify( backupService ).doIncrementalBackupOrFallbackToFull(
                    eq( host ),
                    eq( port ),
                    eq( new File( "/var/backup/graph" ) ),
                    eq( ConsistencyCheck.DEFAULT ),
                    any( Config.class ),
                    eq( BackupClient.BIG_READ_TIMEOUT ),
                    eq( false )
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
            return asList(
                    uri( "foo://127.0.1.1" ),
                    uri( "single://localhost,ha://not-localhost" ),
                    uri( "single://127.0.0.1:6361,single://127.0.0.1:6362" ),
                    uri( "300.400.500.600" ),
                    uri( "host-name_with*wrong$chars.com" ),
                    uri( "dir://my" ),
                    uri( "dir://my", 10 ),
                    uri( "foo://127.0.1.1" ),
                    uri( "foo://127.0.1.1", 6567 ),
                    uri( "cat://localhost" ),
                    uri( "cat://localhost", 4444 ),
                    uri( "notHA://instance1:,instance2:,instance3", 5454 )
            );
        }

        @Test
        public void shouldThrowForInvalidUri() throws Exception
        {
            // Given
            String[] args = new String[]{"-from", uri, "-to", "/var/backup/graph"};

            try
            {
                // When
                newBackupTool().run( args );
                fail( "Should exit abnormally for '" + uri + "'" );
            }
            catch ( BackupTool.ToolFailureException e )
            {
                // Then
                assertThat( e.getMessage(), equalTo( BackupTool.WRONG_FROM_ADDRESS_SYNTAX ) );
            }

            verifyZeroInteractions( backupService, systemOut );
        }
    }

    @RunWith(Parameterized.class)
    public static class IPv6UriTests extends UriTests
    {
        public IPv6UriTests( String host, Integer port )
        {
            super( host, port );
        }

        @Parameterized.Parameters
        public static List<Object[]> data()
        {
            return asList(
                    uri( "[2001:cdba:0000:0000:0000:0000:3257:9652]" ),
                    uri( "[2001:cdba:0000:0000:0000:0000:3257:9652]", 5656 ),
                    uri( "[2001:cdba:0:0:0:0:3257:9652]" ),
                    uri( "[2001:cdba:0:0:0:0:3257:9652]", 9091 ),
                    uri( "[2001:cdba::3257:9652]" ),
                    uri( "[2001:cdba::3257:9652]", 20 ),
                    uri( "[2001:db8::1]", 9991 ),
                    uri( "[2001:db8::1]", 1990 ),
                    uri( "[::1]" ),
                    uri( "[::1]", 8989 ),
                    uri( "[fe80::]" ),
                    uri( "[fe80::]", 1209 ),
                    uri( "[::ffff:0:0]" ),
                    uri( "[::ffff:0:0]", 4545 ),
                    uri( "[ff02::1:1]" ),
                    uri( "[ff02::1:1]", 6767 ),
                    uri( "[2002::]" ),
                    uri( "[2002::]", 3040 )
            );
        }

        @Test
        public void shouldExecuteBackupWithValidUri() throws Exception
        {
            // Given
            String[] args = new String[]{"-host", host, "-port", String.valueOf( port ), "-to", "/var/backup/graph"};

            // When
            newBackupTool().run( args );

            // Then
            verify( backupService ).doIncrementalBackupOrFallbackToFull(
                    eq( host ),
                    eq( port ),
                    eq( new File( "/var/backup/graph" ) ),
                    eq( ConsistencyCheck.DEFAULT ),
                    any( Config.class ),
                    eq( BackupClient.BIG_READ_TIMEOUT ),
                    eq( false )
            );
        }
    }

    private abstract static class UriTests
    {
        final String host;
        final Integer port;
        final String uri;

        final BackupService backupService;
        final PrintStream systemOut;

        UriTests( String host, Integer port )
        {
            this.uri = (port == null) ? host : host + ":" + port;
            this.host = host.replace( "ha://", "" ).replace( "single://", "" );
            this.port = (port == null) ? BackupServer.DEFAULT_PORT : port;

            this.backupService = mock( BackupService.class );
            this.systemOut = mock( PrintStream.class );
        }

        static Object[] uri( String host )
        {
            return uri( host, null );
        }

        static Object[] uri( String host, Integer port )
        {
            return new Object[]{host, port};
        }

        BackupTool newBackupTool() throws Exception
        {
            return spy( new BackupTool( backupService, systemOut ) );
        }
    }
}
