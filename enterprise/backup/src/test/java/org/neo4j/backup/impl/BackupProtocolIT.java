/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import org.junit.Test;

import org.neo4j.backup.TheBackupInterface;
import org.neo4j.backup.impl.BackupClient.BackupRequestType;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static org.jboss.netty.buffer.ChannelBuffers.EMPTY_BUFFER;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class BackupProtocolIT
{
    @Test
    public void shouldGatherForensicsInFullBackupRequest()
    {
        shouldGatherForensicsInFullBackupRequest( true );
    }

    @Test
    public void shouldSkipGatheringForensicsInFullBackupRequest()
    {
        shouldGatherForensicsInFullBackupRequest( false );
    }

    @Test
    public void shouldHandleNoForensicsSpecifiedInFullBackupRequest() throws Exception
    {
        TheBackupInterface backup = mock( TheBackupInterface.class );
        RequestContext ctx = new RequestContext( 0, 1, 0, -1, 12 );
        @SuppressWarnings( "unchecked" )
        TargetCaller<TheBackupInterface, Void> targetCaller =
                (TargetCaller<TheBackupInterface,Void>) BackupRequestType.FULL_BACKUP.getTargetCaller();
        targetCaller.call( backup, ctx, EMPTY_BUFFER, null );
        verify( backup ).fullBackup( any( StoreWriter.class ), eq( false ) );
    }

    private void shouldGatherForensicsInFullBackupRequest( boolean forensics )
    {
        // GIVEN
        Response<Void> response = Response.empty();
        StoreId storeId = response.getStoreId();
        String host = "localhost";
        int port = PortAuthority.allocatePort();
        LifeSupport life = new LifeSupport();

        LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
        NullLogProvider logProvider = NullLogProvider.getInstance();
        ResponseUnpacker responseUnpacker = mock( ResponseUnpacker.class );
        ByteCounterMonitor byteCounterMonitor = mock( ByteCounterMonitor.class );
        RequestMonitor requestMonitor = mock( RequestMonitor.class );
        BackupClient client = new BackupClient( host, port, null, logProvider, storeId, 10_000,
                responseUnpacker, byteCounterMonitor, requestMonitor, reader );
        life.add( client );
        ControlledBackupInterface backup = new ControlledBackupInterface();
        HostnamePort hostnamePort = new HostnamePort( host, port );
        life.add( new BackupServer( backup, hostnamePort, logProvider, byteCounterMonitor, requestMonitor ) );
        life.start();

        try
        {
            // WHEN
            StoreWriter writer = mock( StoreWriter.class );
            client.fullBackup( writer, forensics );

            // THEN
            assertEquals( forensics, backup.receivedForensics );
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
            return Response.empty();
        }

        @Override
        public Response<Void> incrementalBackup( RequestContext context )
        {
            throw new UnsupportedOperationException( "Should be required" );
        }
    }
}
