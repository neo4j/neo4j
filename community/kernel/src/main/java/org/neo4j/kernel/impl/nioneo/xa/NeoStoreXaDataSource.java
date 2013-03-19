/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.UpdateableSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.Store;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.persistence.IdGenerationFailedException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsExtractor;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;

/**
 * A <CODE>NeoStoreXaDataSource</CODE> is a factory for
 * {@link NeoStoreXaConnection NeoStoreXaConnections}.
 * <p>
 * The {@link NioNeoDbPersistenceSource} will create a <CODE>NeoStoreXaDataSoruce</CODE>
 * and then Neo4j kernel will use it to create {@link XaConnection XaConnections} and
 * {@link XaResource XaResources} when running transactions and performing
 * operations on the node space.
 */
public class NeoStoreXaDataSource extends LogBackedXaDataSource
{
    public static final String DEFAULT_DATA_SOURCE_NAME = "nioneodb";

    public static abstract class Configuration
        extends LogBackedXaDataSource.Configuration
    {
        public static final Setting<Boolean> read_only= GraphDatabaseSettings.read_only;
        public static final Setting<File> store_dir = InternalAbstractGraphDatabase.Configuration.store_dir;
        public static final Setting<File> neo_store = InternalAbstractGraphDatabase.Configuration.neo_store;
        public static final Setting<File> logical_log = InternalAbstractGraphDatabase.Configuration.logical_log;
    }

    public static final byte BRANCH_ID[] = UTF8.encode( "414141" );
    public static final String LOGICAL_LOG_DEFAULT_NAME = "nioneo_logical.log";

    private final StoreFactory storeFactory;
    private final XaFactory xaFactory;
    private final JobScheduler scheduler;
    private final UpdateableSchemaState updateableSchemaState;

    private final Config config;
    private final LifeSupport life = new LifeSupport();

    private NeoStore neoStore;
    private IndexingService indexingService;
    private XaContainer xaContainer;
    private ArrayMap<Class<?>,Store> idGenerators;

    private final LockManager lockManager;
    private File storeDir;
    private boolean readOnly;

    private final TransactionInterceptorProviders providers;

    private boolean logApplied = false;

    private final StringLogger msgLog;
    private final TransactionStateFactory stateFactory;
    private final CacheAccessBackDoor cacheAccess;
    private final Logging logging;
    private final DependencyResolver dependencyResolver;

    private enum Diagnostics implements DiagnosticsExtractor<NeoStoreXaDataSource>
    {
        NEO_STORE_VERSIONS( "Store versions:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logVersions( log );
            }
        },
        NEO_STORE_ID_USAGE( "Id usage:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logIdUsage( log );
            }
        },
        PERSISTENCE_WINDOW_POOL_STATS( "Persistence Window Pool stats:" )
        {
            @Override
            void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log )
            {
                source.neoStore.logAllWindowPoolStats( log );
            }

            @Override
            boolean applicable( DiagnosticsPhase phase )
            {
                return phase.isExplicitlyRequested();
            }
        };

        private final String message;

        private Diagnostics( String message )
        {
            this.message = message;
        }

        @Override
        public void dumpDiagnostics( final NeoStoreXaDataSource source, DiagnosticsPhase phase, StringLogger log )
        {
            if ( applicable( phase ) )
            {
                log.logLongMessage( message, new Visitor<StringLogger.LineLogger>()
                {
                    @Override
                    public boolean visit( StringLogger.LineLogger logger )
                    {
                        dump( source, logger );
                        return false;
                    }
                }, true );
            }
        }

        boolean applicable( DiagnosticsPhase phase )
        {
            return phase.isInitialization() || phase.isExplicitlyRequested();
        }

        abstract void dump( NeoStoreXaDataSource source, StringLogger.LineLogger log );
    }

    /**
     * Creates a <CODE>NeoStoreXaDataSource</CODE> using configuration from
     * <CODE>params</CODE>. First the map is checked for the parameter
     * <CODE>config</CODE>.
     * If that parameter exists a config file with that value is loaded (via
     * {@link Properties#load}). Any parameter that exist in the config file
     * and in the map passed into this constructor will take the value from the
     * map.
     * <p>
     * If <CODE>config</CODE> parameter is set but file doesn't exist an
     * <CODE>IOException</CODE> is thrown. If any problem is found with that
     * configuration file or Neo4j store can't be loaded an <CODE>IOException is
     * thrown</CODE>.
     *
     * @throws IOException
     *             If unable to create data source
     */
    public NeoStoreXaDataSource( Config config, StoreFactory sf, LockManager lockManager,
                                 StringLogger stringLogger, XaFactory xaFactory, TransactionStateFactory stateFactory,
                                 CacheAccessBackDoor cacheAccess, TransactionInterceptorProviders providers,
                                 JobScheduler scheduler, Logging logging,
                                 UpdateableSchemaState updateableSchemaState,
                                 DependencyResolver dependencyResolver )
            throws IOException
    {
        super( BRANCH_ID, Config.DEFAULT_DATA_SOURCE_NAME );
        this.config = config;
        this.stateFactory = stateFactory;
        this.cacheAccess = cacheAccess;
        this.dependencyResolver = dependencyResolver;
        this.providers = providers;
        this.scheduler = scheduler;
        this.logging = logging;

        readOnly = config.get( Configuration.read_only );
        this.lockManager = lockManager;
        msgLog = stringLogger;
        this.storeFactory = sf;
        this.xaFactory = xaFactory;
        this.updateableSchemaState = updateableSchemaState;

    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start() throws IOException
    {
        readOnly = config.get( Configuration.read_only );

        storeDir = config.get( Configuration.store_dir );
        File store = config.get( Configuration.neo_store );
        storeFactory.ensureStoreExists();

        final TransactionFactory tf;
        if ( providers.shouldInterceptCommitting() )
        {
            tf = new InterceptingTransactionFactory();
        }
        else
        {
            tf = new TransactionFactory();
        }
        neoStore = storeFactory.newNeoStore( store );

        SchemaIndexProvider indexProvider = dependencyResolver.resolveDependency( SchemaIndexProvider.class,
                HIGHEST_PRIORITIZED_OR_NONE );
        
        indexingService = life.add( new IndexingService( scheduler, indexProvider,
                new NeoStoreIndexStoreView( neoStore ), updateableSchemaState, logging ) );
        
        xaContainer = xaFactory.newXaContainer(this, config.get( Configuration.logical_log ),
                new CommandFactory( neoStore, indexingService ), tf, stateFactory, providers  );

        try
        {
            if ( !readOnly )
            {
                neoStore.setRecoveredStatus( true );
                try
                {
                    indexingService.initIndexes( loadIndexRules() );
                    xaContainer.openLogicalLog();
                }
                finally
                {
                    neoStore.setRecoveredStatus( false );
                }
            }
            if ( !xaContainer.getResourceManager().hasRecoveredTransactions() )
            {
                neoStore.makeStoreOk();
            }
            else
            {
                msgLog.debug( "Waiting for TM to take care of recovered " +
                        "transactions." );
            }
            idGenerators = new ArrayMap<Class<?>,Store>( (byte)5, false, false );
            this.idGenerators.put( Node.class, neoStore.getNodeStore() );
            this.idGenerators.put( Relationship.class,
                neoStore.getRelationshipStore() );
            this.idGenerators.put( RelationshipType.class,
                neoStore.getRelationshipTypeStore() );
            this.idGenerators.put( PropertyStore.class,
                neoStore.getPropertyStore() );
            this.idGenerators.put( PropertyIndex.class,
                                   neoStore.getPropertyStore().getIndexStore() );
            setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );

            life.start();
        }
        catch ( Throwable e )
        {   // Something unexpected happened during startup
            try
            {   // Close the neostore, so that locks are released properly
                neoStore.close();
            }
            catch ( Exception closeException )
            {
                msgLog.logMessage( "Couldn't close neostore after startup failure" );
            }
            throw Exceptions.launderedException( e );
        }
    }

    public NeoStore getNeoStore()
    {
        return neoStore;
    }

    public IndexingService getIndexService()
    {
        return indexingService;
    }

    @Override
    public void stop()
    {
        super.stop();
        life.stop();
        if ( !readOnly )
        {
            indexingService.flushAll();
            neoStore.flushAll();
        }
        xaContainer.close();
        if ( logApplied )
        {
            neoStore.rebuildIdGenerators();
            logApplied = false;
        }
        neoStore.close();
        msgLog.info( "NeoStore closed" );
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }

    public StoreId getStoreId()
    {
        return neoStore.getStoreId();
    }

    @Override
    public NeoStoreXaConnection getXaConnection()
    {
        return new NeoStoreXaConnection( neoStore,
            xaContainer.getResourceManager(), getBranchId() );
    }

    private static class CommandFactory extends XaCommandFactory
    {
        private final NeoStore neoStore;
        private final IndexingService indexingService;

        CommandFactory( NeoStore neoStore, IndexingService indexingService )
        {
            this.neoStore = neoStore;
            this.indexingService = indexingService;
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel byteChannel,
            ByteBuffer buffer ) throws IOException
        {
            return Command.readCommand( neoStore, indexingService, byteChannel, buffer );
        }
    }

    private class InterceptingTransactionFactory extends TransactionFactory
    {
        @Override
        public XaTransaction create( int identifier, TransactionState state )
        {
            TransactionInterceptor first = providers.resolveChain( NeoStoreXaDataSource.this );
            return new InterceptingWriteTransaction( identifier, getLogicalLog(), neoStore, state, cacheAccess,
                    indexingService, lockManager, first );
        }
    }

    private class TransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( int identifier, TransactionState state )
        {
            return new WriteTransaction( identifier, getLogicalLog(), state,
                neoStore, cacheAccess, indexingService );
        }

        @Override
        public void recoveryComplete()
        {
            msgLog.debug( "Recovery complete, "
                    + "all transactions have been resolved" );
            msgLog.debug( "Rebuilding id generators as needed. "
                    + "This can take a while for large stores..." );
            neoStore.flushAll();
            neoStore.makeStoreOk();
            neoStore.setVersion( xaContainer.getLogicalLog().getHighestLogVersion() );
            msgLog.debug( "Rebuild of id generators complete." );
        }

        @Override
        public long getCurrentVersion()
        {
            return neoStore.getVersion();
        }

        @Override
        public long getAndSetNewVersion()
        {
            return neoStore.incrementVersion();
        }
        
        @Override
        public void setVersion( long version )
        {
            neoStore.setVersion( version );
        }

        @Override
        public void flushAll()
        {
            neoStore.flushAll();
            indexingService.flushAll();
        }

        @Override
        public long getLastCommittedTx()
        {
            return neoStore.getLastCommittedTx();
        }
    }

    public long nextId( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );

        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.nextId();
    }

    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getHighestPossibleIdInUse();
    }

    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        Store store = idGenerators.get( clazz );
        if ( store == null )
        {
            throw new IdGenerationFailedException( "No IdGenerator for: "
                + clazz );
        }
        return store.getNumberOfIdsInUse();
    }

    public String getStoreDir()
    {
        return storeDir.getPath();
    }

    @Override
    public long getCreationTime()
    {
        return neoStore.getCreationTime();
    }

    @Override
    public long getRandomIdentifier()
    {
        return neoStore.getRandomNumber();
    }

    @Override
    public long getCurrentLogVersion()
    {
        return neoStore.getVersion();
    }

    public long incrementAndGetLogVersion()
    {
        return neoStore.incrementVersion();
    }

    public void setCurrentLogVersion( long version )
    {
        neoStore.setVersion(version);
    }

    // used for testing, do not use.
    @Override
    public void setLastCommittedTxId( long txId )
    {
        neoStore.setRecoveredStatus( true );
        try
        {
            neoStore.setLastCommittedTx( txId );
        }
        finally
        {
            neoStore.setRecoveredStatus( false );
        }
    }

    ReadTransaction getReadOnlyTransaction()
    {
        return new ReadTransaction( neoStore );
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public List<WindowPoolStats> getWindowPoolStats()
    {
        return neoStore.getAllWindowPoolStats();
    }

    @Override
    public long getLastCommittedTxId()
    {
        return neoStore.getLastCommittedTx();
    }

    @Override
    public XaContainer getXaContainer()
    {
        return xaContainer;
    }

    @Override
    public boolean setRecovered( boolean recovered )
    {
        boolean currentValue = neoStore.isInRecoveryMode();
        neoStore.setRecoveredStatus( true );
        return currentValue;
    }

    @Override
    public ClosableIterable<File> listStoreFiles( boolean includeLogicalLogs )
    {
        final Collection<File> files = new ArrayList<File>();
        File neostoreFile = null;
        Pattern logFilePattern = getXaContainer().getLogicalLog().getHistoryFileNamePattern();
        for ( File dbFile : storeDir.listFiles() )
        {
            String name = dbFile.getName();
            // To filter for "neostore" is quite future proof, but the "index.db" file
            // maybe should be
            if ( dbFile.isFile() )
            {
                if ( name.equals( NeoStore.DEFAULT_NAME ) )
                {
                    neostoreFile = dbFile;
                }
                else if ( (name.startsWith( NeoStore.DEFAULT_NAME ) ||
                        name.equals( IndexStore.INDEX_DB_FILE_NAME )) && !name.endsWith( ".id" ) )
                {   // Store files
                    files.add( dbFile );
                }
                else if ( includeLogicalLogs && logFilePattern.matcher( dbFile.getName() ).matches() )
                {   // Logs
                    files.add( dbFile );
                }
            }
        }
        files.add( neostoreFile );

        return new ClosableIterable<File>()
        {

            @Override
            public Iterator<File> iterator()
            {
                return files.iterator();
            }

            @Override
            public void close()
            {
            }
        };
    }

    public void logStoreVersions()
    {
 // TODO This needs to be reconciled with new Diagnostics       neoStore.logVersions();
    }

    public void logIdUsage()
    {
 // TODO This needs to be reconciled with new Diagnostics       neoStore.logIdUsage();
    }

    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        manager.registerAll( Diagnostics.class, this );
    }

    private Iterable<IndexRule> loadIndexRules()
    {
        return map( new Function<SchemaRule, IndexRule>()
        {
            @Override
            public IndexRule apply( SchemaRule schemaRule )
            {
                return (IndexRule) schemaRule;
            }
        }, filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule item )
            {
                return item.getKind() == SchemaRule.Kind.INDEX_RULE;
            }
        }, neoStore.getSchemaStore().loadAll() ) );
    }
}
