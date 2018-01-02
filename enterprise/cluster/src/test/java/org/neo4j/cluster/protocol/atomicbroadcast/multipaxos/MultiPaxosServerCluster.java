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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import static org.neo4j.test.StreamConsumer.PRINT_FAILURES;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.ProcessStreamHandler;

/**
 * TODO
 */
public class MultiPaxosServerCluster
{
    public static void main( String[] args )
        throws IOException, InterruptedException
    {
        new MultiPaxosServerCluster(3);
    }

    List<PaxosServer> servers = new ArrayList<PaxosServer>(  );

    public MultiPaxosServerCluster(int count)
        throws IOException, InterruptedException
    {
        String processCommand = "java "+ MultiPaxosServer.class.getName();
        for (int i = 0; i < count; i++)
        {
            Process server = Runtime.getRuntime().exec( processCommand, new String[]{"CLASSPATH=\"" + System.getProperty( "java.class.path" )+"\""} );
            sleep( 500 );
            servers.add( new PaxosServer( server, "["+(i+1)+"] " ) );
        }

        sleep( 3000 );
//        all( "logging org.neo4j.kernel.ha2.protocol off" );
        send( 1, "create default" );
        send( 2, "join neo4j://localhost:5001" );
        send( 3, "join neo4j://localhost:5001" );

        sleep( 6000 );

        send( 1, "broadcast hello" );

        sleep( 10000 );

        send( 1, "leave");
        sleep( 1000 );
        send( 2, "leave");
        sleep( 1000 );
        send( 1, "join neo4j://localhost:5003");
        send( 2, "join neo4j://localhost:5003");
        sleep( 6000 );
        send( 3, "promote neo4j://127.0.0.1:5001 coordinator" );
        sleep( 6000 );

        send( 1, "broadcast hello2" );

        for( int i = 0; i < servers.size(); i++ )
        {
            sleep( 3000 );
            PaxosServer paxosServer = servers.get( i );
            paxosServer.quit();
        }
    }

    private void sleep( int i )
        throws InterruptedException
    {
        Thread.sleep( i );
    }

    private void send( int i, String s )
        throws IOException
    {
        servers.get( i-1 ).command( s );
    }

    private void all(String command)
        throws IOException
    {
        for( PaxosServer server : servers )
        {
            server.command( command );
        }
    }

    public static class PaxosServer
    {
        Process server;
        ProcessStreamHandler handler;
        BufferedWriter writer;

        public PaxosServer( Process server, String prefix )
        {
            this.server = server;
            handler = new ProcessStreamHandler( server, false, prefix, PRINT_FAILURES );
            handler.launch();
            writer = new BufferedWriter( new OutputStreamWriter(server.getOutputStream()));
        }

        public void command(String cmd)
            throws IOException
        {
            writer.write( cmd+"\n" );
            writer.flush();
        }

        public void quit()
            throws IOException
        {
            command( "quit" );
            handler.done();
        }
    }
}
