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
package org.neo4j.backup;

import org.junit.Test;

import org.neo4j.backup.BackupClient.BackupRequestType;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;

import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;


public class BackupProtocolTest
{
    @Test
    public void shouldGatherForensicsInFullBackupRequest() throws Exception
    {
        shouldGatherForensicsInFullBackupRequest( true );
    }

    @Test
    public void shouldSkipGatheringForensicsInFullBackupRequest() throws Exception
    {
        shouldGatherForensicsInFullBackupRequest( false );
    }

    @Test
    public void shouldHandleNoForensicsSpecifiedInFullBackupRequest() throws Exception
    {
        TheBackupInterface backup = mock( TheBackupInterface.class );
        RequestContext ctx = new RequestContext( 0, 1, 0, new Tx[]{ new Tx( DEFAULT_DATA_SOURCE_NAME, 1)}, -1, 12 );
        BackupRequestType.FULL_BACKUP.getTargetCaller().call( backup, ctx, EMPTY_BUFFER, null );
        verify( backup ).fullBackup( any( StoreWriter.class ), eq( false ) );
    }

    private void shouldGatherForensicsInFullBackupRequest( boolean forensics ) throws Exception
    {
        // GIVEN
        StoreId storeId = new StoreId();
        DevNullLoggingService logging = new DevNullLoggingService();
        Monitors monitors = new Monitors();
        String host = "localhost";
        int port = BackupServer.DEFAULT_PORT;
        LifeSupport life = new LifeSupport();

        BackupClient client = life.add( new BackupClient( host, port, logging,
                monitors, storeId ) );
        ControlledBackupInterface backup = new ControlledBackupInterface();
        BackupServer server = life.add( new BackupServer( backup, new HostnamePort( host, port ),
                logging, monitors ) );
        life.start();

        try
        {
            // WHEN
            StoreWriter writer = mock( StoreWriter.class );
            client.fullBackup( writer, forensics );

            // THEN
            assertEquals( forensics, backup.receivedForensics.booleanValue() );
        }
        finally
        {
            life.shutdown();
        }
    }

    private static class ControlledBackupInterface implements TheBackupInterface
    {
        private Boolean receivedForensics;

        @Override
        public Response<Void> fullBackup( StoreWriter writer, boolean forensics )
        {
            this.receivedForensics = forensics;
            writer.done();
            return new Response<>( null, new StoreId(), TransactionStream.EMPTY, ResourceReleaser.NO_OP );
        }

        @Override
        public Response<Void> incrementalBackup( RequestContext context )
        {
            throw new UnsupportedOperationException( "Should be required" );
        }
    }
}
