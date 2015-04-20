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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

public class RecoveryTest
{
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    public final @Rule
    TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final LogVersionRepository logVersionRepository = new DeadSimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 5L, 0 );

    @Test
    public void shouldRecoverExistingData() throws Exception
    {
        String name = "log";
        File file = new File( directory.directory(), name + ".1" );
        final int logVersion = 1;
        writeSomeData( file, new Visitor<ByteBuffer, IOException>()
        {
            @Override
            public boolean visit( ByteBuffer buffer ) throws IOException
            {
                writeLogHeader( buffer, logVersion, 3 );
                buffer.clear();
                buffer.position( LOG_HEADER_SIZE );
                buffer.put( (byte) 2 );
                buffer.putInt( 23324 );
                return true;
            }
        } );

        LifeSupport life = new LifeSupport();
        final PhysicalLogFiles logFiles = new PhysicalLogFiles( directory.directory(), name, fs );
        Recovery.Monitor monitor = mock( Recovery.Monitor.class );
        try
        {
            life.add(new Recovery( new Recovery.SPI()
            {
                @Override
                public void forceEverything()
                {

                }

                @Override
                public long getCurrentLogVersion()
                {
                    return logVersionRepository.getCurrentLogVersion();
                }

                @Override
                public Visitor<LogVersionedStoreChannel, IOException> getRecoverer()
                {
                    return new Visitor<LogVersionedStoreChannel, IOException>()
                                            {
                                                @Override
                                                public boolean visit( LogVersionedStoreChannel element ) throws IOException
                                                {
                                                    ReadableVersionableLogChannel recoveredDataChannel =
                                                            new ReadAheadLogChannel( element, NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

                                                    assertEquals( (byte) 2, recoveredDataChannel.get() );
                                                    assertEquals( 23324, recoveredDataChannel.getInt() );
                                                    try
                                                    {
                                                        recoveredDataChannel.get();
                                                        fail( "There should be no more" );
                                                    }
                                                    catch ( ReadPastEndException e )
                                                    {   // Good
                                                    }
                                                    return true;
                                                }
                                            } ;
                }

                @Override
                public PhysicalLogVersionedStoreChannel getLogFile( long recoveryVersion ) throws IOException
                {
                    return PhysicalLogFile.openForVersion( logFiles, fs,
                                                                    recoveryVersion );
                }
            }, monitor ));

            life.add( new PhysicalLogFile( fs, logFiles, 50, transactionIdStore, logVersionRepository, mock( PhysicalLogFile.Monitor.class),
                    new TransactionMetadataCache( 10, 100 )) );

            life.start();

            InOrder order = inOrder( monitor );
            order.verify( monitor, times( 1 ) ).recoveryRequired( logVersion );
            order.verify( monitor, times( 1 ) ).recoveryCompleted();
        }
        finally
        {
            life.shutdown();
        }
    }

    private void writeSomeData( File file, Visitor<ByteBuffer, IOException> visitor ) throws IOException
    {
        try ( StoreChannel channel = fs.open( file, "rw" ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 1024 );
            visitor.visit( buffer );
            buffer.flip();
            channel.write( buffer );
        }
    }
}
