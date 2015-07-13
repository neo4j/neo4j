/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.socket.integration;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.ndp.runtime.Sessions;
import org.neo4j.ndp.runtime.internal.StandardSessions;
import org.neo4j.ndp.transport.socket.NettyServer;
import org.neo4j.ndp.transport.socket.SocketProtocol;
import org.neo4j.ndp.transport.socket.SocketProtocolV1;
import org.neo4j.ndp.transport.socket.SocketTransport;
import org.neo4j.ndp.transport.socket.WebSocketTransport;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

import static java.util.Arrays.asList;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;

public class Neo4jWithSocket implements TestRule
{
    private final LifeSupport life = new LifeSupport();
    private SocketTransport socketTransport;
    private WebSocketTransport wsTransport;

    public HostnamePort address()
    {
        return socketTransport.address();
    }

    @Override
    public Statement apply( final Statement statement, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                final GraphDatabaseService gdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
                final GraphDatabaseAPI api = ((GraphDatabaseAPI) gdb);
                final LogService logging = api.getDependencyResolver().resolveDependency( LogService.class );
                final UsageData usageData = api.getDependencyResolver().resolveDependency( UsageData.class );
                final JobScheduler scheduler = api.getDependencyResolver().resolveDependency( JobScheduler.class );

                final Sessions sessions = life.add( new StandardSessions( api, usageData, logging ) );

                PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> availableVersions = longObjectMap();
                availableVersions.put( SocketProtocolV1.VERSION, new Function<Channel, SocketProtocol>()
                {
                    @Override
                    public SocketProtocol apply( Channel channel )
                    {
                        return new SocketProtocolV1( logging, sessions.newSession(), channel );
                    }
                } );

                SelfSignedCertificate ssc = new SelfSignedCertificate();
                SslContext sslCtx = SslContextBuilder.forServer( ssc.certificate(), ssc.privateKey() ).build();

                // Start services
                socketTransport = new SocketTransport( new HostnamePort( "localhost:7687" ), sslCtx, logging.getUserLogProvider(), availableVersions );
                wsTransport = new WebSocketTransport( new HostnamePort( "localhost:7688" ), sslCtx, logging.getUserLogProvider(), availableVersions );
                life.add( new NettyServer( scheduler.threadFactory( JobScheduler.Groups.gapNetworkIO ), asList(
                        socketTransport,
                        wsTransport )) );

                life.start();
                try
                {
                    statement.evaluate();
                }
                finally
                {
                    life.shutdown();
                    gdb.shutdown();
                }
            }
        };
    }
}
