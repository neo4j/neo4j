/*
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.com.ComException;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith( Parameterized.class )
public class BackupProtocolTest
{
    private final NullLogProvider logProvider = NullLogProvider.getInstance();
    private final ByteCounterMonitor byteCounterMonitor = mock( ByteCounterMonitor.class );
    private final RequestMonitor requestMonitor = mock( RequestMonitor.class );
    private final ResponseUnpacker responseUnpacker = mock( ResponseUnpacker.class );

    @Parameterized.Parameter()
    public boolean forensics;

    @Parameterized.Parameters( name = "forensics={0}")
    public static Collection<Boolean> params()
    {
        return Arrays.asList( Boolean.TRUE, Boolean.FALSE );
    }

    @Test
    public void shouldGatherForensicsInFullBackupRequest() throws Exception
    {
        // GIVEN
        StoreId storeId = Response.EMPTY.getStoreId();
        HostnamePort hostnamePort = new HostnamePort( "localhost", BackupServer.DEFAULT_PORT );

        AtomicBoolean repeat = new AtomicBoolean();
        AtomicInteger attempts = new AtomicInteger();
        do
        {
            BackupClient client =
                    new BackupClient( hostnamePort.getHost(), hostnamePort.getPort(), null, logProvider, storeId, 1000,
                            responseUnpacker, byteCounterMonitor, requestMonitor, new VersionAwareLogEntryReader<>() );
            ControlledBackupInterface backup = new ControlledBackupInterface();
            BackupServer backupServer =
                    new BackupServer( backup, hostnamePort, logProvider, byteCounterMonitor, requestMonitor );

            repeat.set( false );

            try ( Lifespan life = new Lifespan( client, backupServer ) )
            {
                // WHEN
                StoreWriter writer = mock( StoreWriter.class );
                client.fullBackup( writer, forensics );

                // THEN
                assertEquals( forensics, backup.receivedForensics );
            }
            catch ( ComException ex )
            {
                // let's try again
                repeat.set( true );
                attempts.incrementAndGet();
            }
        }
        while ( repeat.get() && attempts.get() < 5 );

        if ( repeat.get() )
        {
            fail( "Always failed after " + attempts.get() + "attempts." );
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
