/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.ha.upgrade;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.Server;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.CleanupRule;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.com.master.MasterImpl.Monitor;

public class MasterClientTest
{
    private static final String MASTER_SERVER_HOST = "localhost";
    private static final int MASTER_SERVER_PORT = 9191;
    private static final int CHUNK_SIZE = 1024;
    private static final int TIMEOUT = 2000;

    @Rule
    public final CleanupRule cleanupRule = new CleanupRule();

    @Test(expected = MismatchingStoreIdException.class)
    public void newClientsShouldNotIgnoreStoreIdDifferences() throws Throwable
    {
        // Given
        MasterImpl.SPI masterImplSPI = mock( MasterImpl.SPI.class );
        when( masterImplSPI.storeId() ).thenReturn( new StoreId( 1, 2, 3, 4 ) );
        when( masterImplSPI.getMasterIdForCommittedTx( anyLong() ) ).thenReturn( Pair.of( 1, 5L ) );

        MasterServer masterServer = cleanupRule.add( newMasterServer( masterImplSPI ) );
        masterServer.init();
        masterServer.start();

        StoreId storeId = new StoreId( 5, 6, 7, 8 );
        MasterClient214 masterClient214 = cleanupRule.add( newMasterClient214( storeId ) );
        masterClient214.init();
        masterClient214.start();

        // When
        masterClient214.handshake( 1, storeId );
    }

    private static MasterServer newMasterServer( MasterImpl.SPI masterImplSPI )
    {
        MasterImpl master = new MasterImpl( masterImplSPI, mock( Monitor.class ),
                mock( Logging.class ), masterConfig() );

        return new MasterServer( master, mock( Logging.class, RETURNS_MOCKS ), masterServerConfiguration(),
                mock( TxChecksumVerifier.class ), new Monitors() );
    }

    private static MasterClient214 newMasterClient214( StoreId storeId )
    {
        return new MasterClient214( MASTER_SERVER_HOST, MASTER_SERVER_PORT, mock( Logging.class, RETURNS_MOCKS ),
                new Monitors(), storeId, TIMEOUT, TIMEOUT, 1, CHUNK_SIZE );
    }

    private static Config masterConfig()
    {
        return new Config( stringMap( ClusterSettings.server_id.name(), "1" ) );
    }

    private static Server.Configuration masterServerConfiguration()
    {
        return new Server.Configuration()
        {
            @Override
            public long getOldChannelThreshold()
            {
                return -1;
            }

            @Override
            public int getMaxConcurrentTransactions()
            {
                return 1;
            }

            @Override
            public int getChunkSize()
            {
                return CHUNK_SIZE;
            }

            @Override
            public HostnamePort getServerAddress()
            {
                return new HostnamePort( MASTER_SERVER_HOST, MASTER_SERVER_PORT );
            }
        };
    }
}
