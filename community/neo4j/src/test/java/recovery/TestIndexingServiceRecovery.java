/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package recovery;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.test.LogTestUtils;
import org.neo4j.test.LogTestUtils.LogHook;
import org.neo4j.test.LogTestUtils.LogHookAdapter;

import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;
import static org.neo4j.test.LogTestUtils.oneOrTwo;
import static org.neo4j.test.ProcessUtil.executeSubProcess;
import static org.neo4j.test.TargetDirectory.forTest;

/**
 * There was an issue with recovery where IndexingService#start was called too early, before
 * {@link NeoStore} had been recovered. This would effectively disable recovery of schema related
 * indexes and constraints. The startup order in a faulty scenario was like this:
 *
 * <pre>
 * XaDsMgr#start
 * NodeStore#setStoreNotOk
 * NodeStore#setStoreNotOk
 * IndexingService#initIndexes
 * ... nioneo_logical.log.1/2 is read and store recovered as much as possible ...
 * IndexingService#start
 * TxManager#start
 * TxManager#doRecovery
 * NodeStore#makeStoreOk <-- happens here since there are one or more 2PC transactions the data source
 *                           didn't know what to do with above
 * </pre>
 *
 * Where it's essential that TxManager#doRecovery and NodeStore#makeStoreOk must have been called
 * before IndexingService#start (where deferred index recovery is performed).
 * IndexingService recovery uses NodeStore#forceGetRecord which just returns a "not in use" node record
 * for any requested id if not yet recovered, so this faulty startup order was hidden by that.
 * Here, we'll try to expose it.
 */
public class TestIndexingServiceRecovery
{
    @Test
    public void shouldRecoverSchemaIndexesAfterNeoStoreFullyRecovered() throws Exception
    {
        // GIVEN a db with schema and some data in it
        File storeDir = forTest( getClass() ).makeGraphDbDir();
        Long[] nodeIds = createSchemaData( storeDir );
        // crashed store that has at least one 2PC transaction
        executeSubProcess( getClass(), 30, SECONDS, args( storeDir.getAbsolutePath(), nodeIds ) );
        // and that 2PC transaction is missing the 2PC commit record in the neostore data source,
        // which simulates a 2PC transaction where not all data sources have committed fully.
        makeItLookLikeNeoStoreDataSourceDidntCommitLastTransaction( storeDir );

        // WHEN recovering that store
        Pair<Collection<Long>, Collection<NodePropertyUpdate>> updatesSeenDuringRecovery = recoverStore( storeDir );

        // THEN the index should be recovered with the expected data from neostore
        int labelId = 0, propertyKey = 0; // TODO bad assuming id 0
        assertEquals( "Test contains invalid assumptions. Recovery process should have seen updates around these nodes",
                asSet( nodeIds ), updatesSeenDuringRecovery.first() );
        assertEquals( asSet( add( nodeIds[0], propertyKey, value, new long[] {labelId} ) ),
                updatesSeenDuringRecovery.other() );
    }

    // The process that will crash the db uses this main method
    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[0] );

        // Set the property that we'll want to recover
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( parseLong( args[1] ) );
            node.setProperty( key, value );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // Create an arbitrary 2PC transaction with which we'll use to break shit later on
            Node otherNode = db.getNodeById( parseLong( args[2] ) );
            otherNode.setProperty( "some key", "some value" );
            db.index().forNodes( "nodes" ).add( otherNode, key, value );
            tx.success();
        }
        // Crash on purpose
        System.exit( 0 );
    }

    private Pair<Collection<Long>,Collection<NodePropertyUpdate>> recoverStore( File storeDir )
    {
        final Collection<Long> affectedNodeIds = new HashSet<>();
        final Collection<NodePropertyUpdate> allUpdates = new HashSet<>();
        final IndexingService.Monitor recoveryMonitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void applyingRecoveredData( Collection<Long> nodeIds )
            {
                affectedNodeIds.addAll( nodeIds );
            }

            @Override
            public void appliedRecoveredData( Iterable<NodePropertyUpdate> updates )
            {
                for ( NodePropertyUpdate update : updates )
                {
                    allUpdates.add( update );
                }
            }
        };

        GraphDatabaseService db = new GraphDatabaseFactory()
        {
            @SuppressWarnings( "deprecation" )
            @Override
            public GraphDatabaseService newEmbeddedDatabase( String path )
            {
                GraphDatabaseFactoryState state = getStateCopy();
                return new EmbeddedGraphDatabase( path, stringMap(),
                        state.databaseDependencies() )
                {
                    @Override
                    protected void createNeoDataSource( LockService locks )
                    {
                        // Register our little special recovery listener
                        neoDataSource = new NeoStoreXaDataSource( config,
                                locks, storeFactory, logging.getMessagesLog( NeoStoreXaDataSource.class ),
                                xaFactory, stateFactory, transactionInterceptorProviders, jobScheduler, logging,
                                updateableSchemaState, new NonTransactionalTokenNameLookup( labelTokenHolder, propertyKeyTokenHolder ),
                                dependencyResolver, txManager, propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder,
                                persistenceManager, this, transactionEventHandlers, recoveryMonitor,
                                new DefaultFileSystemAbstraction(), new Function<NeoStore, Function<List<LogEntry>, List<LogEntry>>>()

                        {
                            @Override
                            public Function<List<LogEntry>, List<LogEntry>> apply( NeoStore neoStore )
                            {
                                return Functions.identity();
                            }
                        }, storeMigrationProcess );
                        xaDataSourceManager.registerDataSource( neoDataSource );
                    }
                };
            }
        }.newEmbeddedDatabase( storeDir.getAbsolutePath() );
        db.shutdown();
        return Pair.of( affectedNodeIds, allUpdates );
    }

    private Long[] createSchemaData( File storeDir )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().indexFor( label ).on( key ).create();
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 10, SECONDS );
                tx.success();
            }

            long nodeId;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label );
                nodeId = node.getId();
                tx.success();
            }

            long otherNodeId;
            try ( Transaction tx = db.beginTx() )
            {
                otherNodeId = db.createNode().getId();
                tx.success();
            }
            return new Long[] {nodeId, otherNodeId};
        }
        finally
        {
            db.shutdown();
        }
    }

    private void makeItLookLikeNeoStoreDataSourceDidntCommitLastTransaction( File storeDir ) throws IOException
    {
        // Capture the identifier of the last transaction
        File logFile = single( iterator( oneOrTwo( fs, new File( storeDir, LOGICAL_LOG_DEFAULT_NAME ) ) ) );
        final AtomicInteger lastIdentifier = new AtomicInteger();
        filterNeostoreLogicalLog( fs, logFile, LogTestUtils.findLastTransactionIdentifier( lastIdentifier ) );

        // Filter any commit/prepare/done entries from that transaction
        LogHook<LogEntry> prune = new LogHookAdapter<LogEntry>()
        {
            @Override
            public boolean accept( LogEntry item )
            {
                // Let through everything exception COMMIT/DONE for the last tx
                boolean shouldPruneEntry = item.getIdentifier() == lastIdentifier.get() &&
                        (item instanceof LogEntry.Commit || item instanceof LogEntry.Done);
                return !shouldPruneEntry;
            }
        };
        File tempFile = filterNeostoreLogicalLog( fs, logFile, prune );
        fs.deleteFile( logFile );
        fs.renameFile( tempFile, logFile );
    }

    private String[] args( String absolutePath, Long[] nodeIds )
    {
        List<String> result = new ArrayList<>();
        result.add( absolutePath );
        for ( Long id : nodeIds )
        {
            result.add( String.valueOf( id ) );
        }
        return result.toArray( new String[result.size()] );
    }

    @SuppressWarnings( "deprecation" )
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private static final Label label = label( "Label" );
    private static final String key = "key", value = "Value";
}
