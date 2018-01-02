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
package org.neo4j.tools.applytx;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.tools.console.input.ArgsCommand;

import static java.lang.String.format;

import static org.neo4j.helpers.progress.ProgressMonitorFactory.textual;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.RECOVERY;

public class ApplyTransactionsCommand extends ArgsCommand
{
    private final File from;
    private final Provider<GraphDatabaseAPI> to;

    public ApplyTransactionsCommand( File from, Provider<GraphDatabaseAPI> to )
    {
        this.from = from;
        this.to = to;
    }

    @Override
    protected void run( Args args, PrintStream out ) throws Exception
    {
        TransactionIdStore txIdStore = to.instance().getDependencyResolver().resolveDependency(
                TransactionIdStore.class );
        long fromTx = txIdStore.getLastCommittedTransaction().transactionId();
        long toTx;
        if ( args.orphans().isEmpty() )
        {
            throw new IllegalArgumentException( "No tx specified" );
        }

        String whereTo = args.orphans().get( 0 );
        if ( whereTo.equals( "next" ) )
        {
            toTx = fromTx + 1;
        }
        else if ( whereTo.equals( "last" ) )
        {
            toTx = Long.MAX_VALUE;
        }
        else
        {
            toTx = Long.parseLong( whereTo );
        }

        long lastApplied = applyTransactions( from, to.instance(), fromTx, toTx, out );
        out.println( "Applied transactions up to and including " + lastApplied );
    }

    private long applyTransactions( File fromPath, GraphDatabaseAPI toDb, long fromTxExclusive, long toTxInclusive,
            PrintStream out )
            throws IOException, TransactionFailureException
    {
        DependencyResolver resolver = toDb.getDependencyResolver();
        BatchingTransactionRepresentationStoreApplier applier = new BatchingTransactionRepresentationStoreApplier(
                resolver.resolveDependency( IndexingService.class ),
                resolver.resolveDependency( LabelScanStore.class ),
                resolver.resolveDependency( NeoStoresSupplier.class ).get(),
                resolver.resolveDependency( CacheAccessBackDoor.class ),
                resolver.resolveDependency( LockService.class ),
                resolver.resolveDependency( LegacyIndexApplierLookup.class ),
                resolver.resolveDependency( IndexConfigStore.class ),
                resolver.resolveDependency( KernelHealth.class ),
                resolver.resolveDependency( IdOrderingQueue.class ) );
        TransactionRepresentationCommitProcess commitProcess =
                new TransactionRepresentationCommitProcess(
                        resolver.resolveDependency( TransactionAppender.class ),
                        applier,
                        resolver.resolveDependency( IndexUpdatesValidator.class ) );
        LifeSupport life = new LifeSupport();
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem ) )
        {
            LogicalTransactionStore source = life.add(
                    new ReadOnlyTransactionStore( pageCache, fileSystem, fromPath, new Monitors() ) );
            life.start();
            long lastAppliedTx = fromTxExclusive;
            // Some progress if there are more than a couple of transactions to apply
            ProgressListener progress = toTxInclusive - fromTxExclusive >= 100 ?
                                        textual( out ).singlePart( "Application progress",
                                                toTxInclusive - fromTxExclusive ) :
                                        ProgressListener.NONE;
            try ( IOCursor<CommittedTransactionRepresentation> cursor = source.getTransactions( fromTxExclusive + 1 ) )
            {
                while ( cursor.next() )
                {
                    CommittedTransactionRepresentation transaction = cursor.get();
                    try ( LockGroup locks = new LockGroup() )
                    {
                        commitProcess.commit( transaction.getTransactionRepresentation(), locks,
                                CommitEvent.NULL, RECOVERY );
                        progress.add( 1 );
                    }
                    catch ( final Throwable e )
                    {
                        System.err.println( "ERROR applying transaction " + transaction.getCommitEntry().getTxId() );
                        throw e;
                    }
                    lastAppliedTx = transaction.getCommitEntry().getTxId();
                    if ( lastAppliedTx == toTxInclusive )
                    {
                        break;
                    }
                }
            }
            applier.closeBatch();
            return lastAppliedTx;
        }
        finally
        {
            life.shutdown();
        }
    }

    @Override
    public String toString()
    {
        return ArrayUtil.join( new String[] {
                "Applies transaction from the source onto the new db. Example:",
                "  apply last : applies transactions from the currently last applied and up to the last",
                "               transaction of source db",
                "  apply next : applies the next transaction onto the new db",
                "  apply 234  : applies up to and including tx 234 from the source db onto the new db" },
                format( "%n" ) );
    }
}
