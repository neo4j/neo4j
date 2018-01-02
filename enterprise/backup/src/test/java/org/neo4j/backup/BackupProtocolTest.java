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

import org.neo4j.backup.BackupClient.BackupRequestType;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.NullLogProvider;

import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
        RequestContext ctx = new RequestContext( 0, 1, 0, -1, 12 );
        BackupRequestType.FULL_BACKUP.getTargetCaller().call( backup, ctx, EMPTY_BUFFER, null );
        verify( backup ).fullBackup( any( StoreWriter.class ), eq( false ) );
    }

    private void shouldGatherForensicsInFullBackupRequest( boolean forensics ) throws Exception
    {
        // GIVEN
        Response<Void> response = Response.EMPTY;
        StoreId storeId = response.getStoreId();
        String host = "localhost";
        int port = BackupServer.DEFAULT_PORT;
        LifeSupport life = new LifeSupport();

        BackupClient client = life.add( new BackupClient( host, port, null, NullLogProvider.getInstance(), storeId, 1000,
                mock( ResponseUnpacker.class ), mock( ByteCounterMonitor.class ), mock( RequestMonitor.class ) ) );
        ControlledBackupInterface backup = new ControlledBackupInterface();
        life.add( new BackupServer( backup, new HostnamePort( host, port ), NullLogProvider.getInstance(), mock( ByteCounterMonitor.class ),
                mock( RequestMonitor.class )) );
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
            writer.close();
            return Response.EMPTY;
        }

        @Override
        public Response<Void> incrementalBackup( RequestContext context )
        {
            throw new UnsupportedOperationException( "Should be required" );
        }
    }
}
