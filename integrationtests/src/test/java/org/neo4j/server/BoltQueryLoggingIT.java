/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server;

import org.junit.Rule;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.server.configuration.ServerSettings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.ServerTestUtils.createTempDir;

public class BoltQueryLoggingIT
{
    @Rule
    public final Neo4jRule neo4j;

    public BoltQueryLoggingIT() throws IOException
    {
        String tmpDir = createTempDir().getAbsolutePath();
        this.neo4j = new Neo4jRule()
            .withConfig( ServerSettings.http_logging_enabled, "true" )
            .withConfig( LegacySslPolicyConfig.certificates_directory.name(), tmpDir )
            .withConfig( GraphDatabaseSettings.auth_enabled, "false" )
            .withConfig( GraphDatabaseSettings.logs_directory, tmpDir )
            .withConfig( GraphDatabaseSettings.log_queries, "true")
            .withConfig( ServerSettings.script_enabled.name(), Settings.TRUE )
            .withConfig( new BoltConnector( "bolt" ).type, "BOLT" )
            .withConfig( new BoltConnector( "bolt" ).enabled, "true" )
            .withConfig( new BoltConnector( "bolt" ).address, "localhost:" + PortAuthority.allocatePort() )
            .withConfig( new BoltConnector( "bolt" ).encryption_level, "DISABLED" )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
    }

    @Test
    public void shouldLogQueriesViaBolt() throws IOException
    {
        // *** GIVEN ***

        Socket socket = new Socket( "localhost", neo4j.boltURI().getPort() );
        DataInputStream dataIn = new DataInputStream( socket.getInputStream() );
        DataOutputStream dataOut = new DataOutputStream( socket.getOutputStream() );

        // Bolt handshake
        send( dataOut, new byte[] { (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } );
        receive( dataIn, new byte[]{ 0, 0, 0, 1 } );

        // This has been taken from: http://alpha.neohq.net/docs/server-manual/bolt-examples.html

        // Send INIT "MyClient/1.0" { "scheme": "basic", "principal": "neo4j", "credentials": "secret"}
        send( dataOut,
                "00 40 B1 01  8C 4D 79 43  6C 69 65 6E  74 2F 31 2E\n" +
                "30 A3 86 73  63 68 65 6D  65 85 62 61  73 69 63 89\n" +
                "70 72 69 6E  63 69 70 61  6C 85 6E 65  6F 34 6A 8B\n" +
                "63 72 65 64  65 6E 74 69  61 6C 73 86  73 65 63 72\n" +
                "65 74 00 00" );
        // Receive SUCCESS {}
        receiveSuccess( dataIn );

        // *** WHEN ***

        for ( int i = 0; i < 5; i++ )
        {
            // Send RUN "RETURN 1 AS num" {}
            send( dataOut, "00 13 b2 10  8f 52 45 54  55 52 4e 20  31 20 41 53 20 6e 75 6d  a0 00 00" );
            // Receive SUCCESS { "fields": ["num"], "result_available_after": X }
            //non-deterministic so just ignore it here
            receiveSuccess( dataIn );

            //receive( dataIn, "00 0f b1 70  a1 86 66 69  65 6c 64 73  91 83 6e 75 6d 00 00" );

            // Send PULL_ALL
            send( dataOut, "00 02 B0 3F  00 00" );
            // Receive RECORD[1]
            receive( dataIn, "00 04 b1 71  91 01 00 00" );
            // Receive SUCCESS { "type": "r", "result_consumed_after": Y }
            //non-deterministic so just ignore it here
            receiveSuccess(  dataIn );
        }

        // *** THEN ***

        Path queriesLog = neo4j.getConfig().get( GraphDatabaseSettings.log_queries_filename ).toPath();
        List<String> lines = Files.readAllLines( queriesLog );
        assertThat( lines, hasSize( 5 ) );
        for ( String line : lines )
        {
            assertTrue( line.contains( "INFO" ) );
            assertTrue( line.contains( "ms: bolt-session\tbolt\tneo4j\tMyClient/1.0" ) );
            assertTrue( line.contains( "client/127.0.0.1:" ) );
            assertTrue( line.contains( "client/127.0.0.1:" ) );
            assertTrue( line.contains( "server/127.0.0.1:" + neo4j.boltURI().getPort() ) );
            assertTrue( line.contains( " - RETURN 1 AS num - {}" ) );
        }

        // *** CLEAN UP ***

        // Send RESET
        send( dataOut, "00 02 b0 0f 00 00" );
        // Receive SUCCESS {}
        receive( dataIn, "00 03 b1 70  a0 00 00" );

        socket.close();
    }

    private static void send( DataOutputStream dataOut, String toSend ) throws IOException
    {
        send( dataOut, hexBytes( toSend ) );
    }

    private static void send( DataOutputStream dataOut, byte[] bytesToSend ) throws IOException
    {
        dataOut.write( bytesToSend );
        dataOut.flush();
    }

    private void receiveSuccess( DataInputStream dataIn ) throws IOException
    {
        short bytes = dataIn.readShort();
        assertThat(dataIn.readUnsignedByte(), equalTo(0xB1));
        assertThat(dataIn.readUnsignedByte(), equalTo(0x70));
        dataIn.skipBytes( bytes);
    }

    private static void receive( DataInputStream dataIn, String expected ) throws IOException
    {
        receive( dataIn, hexBytes( expected ) );
    }
    private static void receive( DataInputStream dataIn, byte[] expectedBytes ) throws IOException
    {
        byte[] actualBytes = read( dataIn, expectedBytes.length );
        assertThat( actualBytes, equalTo( expectedBytes ) );
    }

    private static byte[] hexBytes( String input )
    {
        String[] pieces = input.trim().split( "\\s+" );
        byte[] result = new byte[pieces.length];
        for ( int i = 0; i < pieces.length; i++ )
        {
            result[i] = hexByte( pieces[i] );
        }
        return result;
    }

    private static byte hexByte( String s )
    {
        int hi = Character.digit( s.charAt( 0 ), 16 ) << 4;
        int lo = Character.digit( s.charAt( 1 ), 16 );
        return (byte) ( hi | lo );
    }

    private static byte[] read( DataInputStream dataIn, int howMany ) throws IOException
    {
        assert howMany > 0;

        byte[] buffer = new byte[howMany];
        int offset = 0;
        while ( offset < howMany )
        {
            int read = dataIn.read( buffer, offset, howMany - offset );
            if ( read == 0 )
            {
                Thread.yield();
            }
            offset += read;
        }
        return buffer;
    }
}
