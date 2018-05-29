/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.scenarios;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.transaction_listen_address;

public class ConnectionInfoIT
{
    private Socket testSocket;

    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );

    @After
    public void teardown() throws IOException
    {
        if ( testSocket != null )
        {
            unbind( testSocket );
        }
    }

    @Test
    public void catchupServerMessage() throws Throwable
    {
        // given
        testSocket = bindPort( "localhost", 4242 );

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        AssertableLogProvider userLogProvider = new AssertableLogProvider();
        CoreSnapshotService snapshotService = mock( CoreSnapshotService.class );
        Config config = Config.defaults()
                .with( singletonMap( transaction_listen_address.name(), ":" + testSocket.getLocalPort() ) );

        CatchupServer catchupServer =
                new CatchupServer( logProvider, userLogProvider, mockSupplier(), mockSupplier(), mockSupplier(),
                        mockSupplier(), mock( BooleanSupplier.class ), snapshotService, config, new Monitors(),
                        mockSupplier(), mock( FileSystemAbstraction.class ), mock( PageCache.class ),
                        new StoreCopyCheckPointMutex(), null );

        //then
        try
        {
            catchupServer.start();
        }
        catch ( Throwable throwable )
        {
            //expected.
        }
        logProvider.assertContainsMessageContaining( "Address is already bound for setting" );
        userLogProvider.assertContainsMessageContaining( "Address is already bound for setting" );
    }

    @SuppressWarnings( "unchecked" )
    private <T> Supplier<T> mockSupplier()
    {
        return mock( Supplier.class );
    }

    private Socket bindPort( String address, int port ) throws IOException
    {
        Socket socket = new Socket();
        socket.bind( new InetSocketAddress( address, port ) );
        return socket;
    }

    private void unbind( Socket socket ) throws IOException
    {
        socket.close();
    }
}
