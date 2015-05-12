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
package org.neo4j.kernel.impl.recovery;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class TestStoreRecoverer
{
    @Test
    public void shouldNotWantToRecoverIntactStore() throws Exception
    {
        File store = null;
        store = createIntactStore();

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store ), is( false ) );
    }

    @Test
    public void shouldWantToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        createLogFileForNextVersionWithSomeDataInIt( store, fileSystem );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store ), is( true ) );
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        createLogFileForNextVersionWithSomeDataInIt( store, fileSystem );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store ), is( true ) );

        // Don't call recoverer.recover, because currently it's hard coded to start an embedded db
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( store.getPath() ).shutdown();

        assertThat( recoverer.recoveryNeededAt( store ), is( false ) );
    }

    private File createIntactStore()
    {
        File storeDir = new File( "dir" );
        fileSystem.mkdirs( storeDir );
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir.getPath() ).shutdown();
        return storeDir;
    }

    public static void createLogFileForNextVersionWithSomeDataInIt( File store, FileSystemAbstraction fileSystem ) throws IOException
    {
        NeoStoreUtil util = new NeoStoreUtil( store, fileSystem );

        LifeSupport life = new LifeSupport();
        DeadSimpleTransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( util.getLastCommittedTx(), 0 );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 10 );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( store, PhysicalLogFile.DEFAULT_NAME, fileSystem );
        LogFile logFile = life.add( new PhysicalLogFile( fileSystem, logFiles, 1000, transactionIdStore,
                new DeadSimpleLogVersionRepository( util.getLogVersion() ), mock( PhysicalLogFile.Monitor.class ),
                positionCache ) );
        life.start();

        try
        {
            PhysicalLogicalTransactionStore transactionStore = new PhysicalLogicalTransactionStore( logFile,
                    LogRotation.NO_ROTATION, positionCache, transactionIdStore, IdOrderingQueue.BYPASS,
                    mock( KernelHealth.class ) );
            life.add( transactionStore );
            TransactionAppender appender = transactionStore.getAppender();
            appender.append( singleNodeTransaction(), LogAppendEvent.NULL );
        }
        finally
        {
            life.shutdown();
        }
    }

    private static TransactionRepresentation singleNodeTransaction()
    {
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( Arrays.asList( createNodeCommand() ) );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, -1 );
        return transaction;
    }

    private static Command createNodeCommand()
    {
        NodeCommand nodeCommand = new NodeCommand();
        long id = 0;
        NodeRecord after = new NodeRecord( id );
        after.setInUse( true );
        nodeCommand.init( new NodeRecord( id ), after );
        return nodeCommand;
    }

    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
}
