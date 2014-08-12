/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreUtil;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.xaframework.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.xaframework.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultTxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.LogFile;
import org.neo4j.kernel.impl.transaction.xaframework.LogRotationControl;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionAppender;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppender;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.util.Providers;
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
        DeadSimpleTransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 2 );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 10 );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( store, PhysicalLogFile.DEFAULT_NAME, fileSystem );
        LogFile logFile = life.add( new PhysicalLogFile( fileSystem, logFiles, 1000,
                LogPruneStrategyFactory.NO_PRUNING, transactionIdStore,
                new DeadSimpleLogVersionRepository( util.getLogVersion() ), mock( PhysicalLogFile.Monitor.class ),
                mock( LogRotationControl.class ), positionCache, mock( Visitor.class ) ) );
        life.start();

        try
        {
            TransactionAppender appender = new PhysicalTransactionAppender( logFile,
                    new DefaultTxIdGenerator( Providers.<TransactionIdStore>singletonProvider( transactionIdStore ) ),
                    positionCache, transactionIdStore );
            appender.append( singleNodeTransaction() );
        }
        finally
        {
            life.shutdown();
        }
    }

    private static TransactionRepresentation singleNodeTransaction()
    {
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( Arrays.asList( createNodeCommand() ) );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0 );
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
