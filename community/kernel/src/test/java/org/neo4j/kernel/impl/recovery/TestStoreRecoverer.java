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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreUtil;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.xaframework.DeadSimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.xaframework.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultTxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.LogFile;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.LogRotationControl;
import org.neo4j.kernel.impl.transaction.xaframework.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionAppender;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionAppender;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.util.Providers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestStoreRecoverer
{
    @Test
    public void shouldNotWantToRecoverIntactStore() throws Exception
    {
        File store = null;
        store = createIntactStore();

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store, logRepository( store ), new HashMap<String, String>() ), is( false ) );
    }

    private LogVersionRepository logRepository( File store )
    {
        return new DeadSimpleLogVersionRepository( new NeoStoreUtil( store, fileSystem ).getLogVersion() );
    }

    @Test
    public void shouldWantToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        createLogFileForNextVersionWithSomeDataInIt( store );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store, logRepository( store ), new HashMap<String, String>() ), is( true ) );
    }

    @Test
    public void shouldBeAbleToRecoverBrokenStore() throws Exception
    {
        File store = createIntactStore();
        createLogFileForNextVersionWithSomeDataInIt( store );

        StoreRecoverer recoverer = new StoreRecoverer( fileSystem );

        assertThat( recoverer.recoveryNeededAt( store, logRepository( store ), new HashMap<String, String>() ), is( true ) );

        // Don't call recoverer.recover, because currently it's hard coded to start an embedded db
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( store.getPath() ).shutdown();

        assertThat( recoverer.recoveryNeededAt( store, logRepository( store ), new HashMap<String, String>() ), is( false ) );
    }

    private File createIntactStore()
    {
        File storeDir = new File( "dir" );
        fileSystem.mkdirs( storeDir );
        new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir.getPath() ).shutdown();
        return storeDir;
    }

    private void createLogFileForNextVersionWithSomeDataInIt( File store ) throws IOException
    {
        NeoStoreUtil util = new NeoStoreUtil( store, fileSystem );

        LifeSupport life = new LifeSupport();
        DeadSimpleTransactionIdStore transactionIdStore = new DeadSimpleTransactionIdStore( 2 );
        TransactionMetadataCache positionCache = new TransactionMetadataCache( 10, 10 );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( store, PhysicalLogFile.DEFAULT_NAME, fileSystem );
        LogFile logFile = life.add( new PhysicalLogFile( fileSystem, logFiles, 1000,
                LogPruneStrategies.NO_PRUNING, transactionIdStore,
                new DeadSimpleLogVersionRepository( util.getLogVersion() ), mock( PhysicalLogFile.Monitor.class ),
                mock( LogRotationControl.class ), positionCache, mock( Visitor.class ) ) );
        life.start();

        try ( TransactionAppender appender = new PhysicalTransactionAppender( logFile,
                new DefaultTxIdGenerator( Providers.<TransactionIdStore>singletonProvider( transactionIdStore ) ),
                positionCache ) )
        {
            appender.append( singleNodeTransaction() );
        }
        finally
        {
            life.shutdown();
        }
    }

    private TransactionRepresentation singleNodeTransaction()
    {
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( Arrays.asList( createNodeCommand() ) );
        transaction.setHeader( new byte[0], 0, 0, 0, 0 );
        return transaction;
    }

    private Command createNodeCommand()
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
