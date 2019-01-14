/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.test.rule;

import java.util.function.Function;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.BatchTransactionApplierFacade;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.command.IndexActivator;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.impl.EphemeralIdGenerator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.test.MockedNeoStores.mockedTokenHolders;

/**
 * Conveniently manages a {@link RecordStorageEngine} in a test. Needs {@link FileSystemAbstraction} and
 * {@link PageCache}, which usually are managed by test rules themselves. That's why they are passed in
 * when {@link #getWith(FileSystemAbstraction, PageCache, DatabaseLayout) getting (constructing)} the engine. Further
 * dependencies can be overridden in that returned builder as well.
 * <p>
 * Keep in mind that this rule must be created BEFORE {@link ConfigurablePageCacheRule} and any file system rule so that
 * shutdown order gets correct.
 */
public class RecordStorageEngineRule extends ExternalResource
{
    private final LifeSupport life = new LifeSupport();

    @Override
    protected void before() throws Throwable
    {
        super.before();
        life.start();
    }

    public Builder getWith( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        return new Builder( fs, pageCache, databaseLayout );
    }

    private RecordStorageEngine get( FileSystemAbstraction fs, PageCache pageCache,
                                     IndexProvider indexProvider, DatabaseHealth databaseHealth, DatabaseLayout databaseLayout,
                                     Function<BatchTransactionApplierFacade, BatchTransactionApplierFacade> transactionApplierTransformer,
                                     Monitors monitors, LockService lockService )
    {
        IdGeneratorFactory idGeneratorFactory = new EphemeralIdGenerator.Factory();
        ExplicitIndexProvider explicitIndexProviderLookup = mock( ExplicitIndexProvider.class );
        when( explicitIndexProviderLookup.allIndexProviders() ).thenReturn( Iterables.empty() );
        IndexConfigStore indexConfigStore = new IndexConfigStore( databaseLayout, fs );
        JobScheduler scheduler = life.add( createScheduler() );
        Config config = Config.defaults( GraphDatabaseSettings.default_schema_provider, indexProvider.getProviderDescriptor().name() );

        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( indexProvider );

        BufferingIdGeneratorFactory bufferingIdGeneratorFactory =
                new BufferingIdGeneratorFactory( idGeneratorFactory, IdReuseEligibility.ALWAYS,
                        new CommunityIdTypeConfigurationProvider() );
        DefaultIndexProviderMap indexProviderMap = new DefaultIndexProviderMap( dependencies, config );
        NullLogProvider nullLogProvider = NullLogProvider.getInstance();
        life.add( indexProviderMap );
        return life.add( new ExtendedRecordStorageEngine( databaseLayout, config, pageCache, fs,
                nullLogProvider, nullLogProvider, mockedTokenHolders(),
                mock( SchemaState.class ), new StandardConstraintSemantics(),
                scheduler, mock( TokenNameLookup.class ), lockService, indexProviderMap,
                IndexingService.NO_MONITOR, databaseHealth, explicitIndexProviderLookup, indexConfigStore,
                new SynchronizedArrayIdOrderingQueue(), idGeneratorFactory,
                new BufferedIdController( bufferingIdGeneratorFactory, scheduler ), transactionApplierTransformer, monitors,
                RecoveryCleanupWorkCollector.immediate(), OperationalMode.single ) );
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        life.shutdown();
        super.after( successful );
    }

    public class Builder
    {
        private final FileSystemAbstraction fs;
        private final PageCache pageCache;
        private DatabaseHealth databaseHealth = new DatabaseHealth(
                new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                NullLog.getInstance() );
        private final DatabaseLayout databaseLayout;
        private Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer =
                applierFacade -> applierFacade;
        private IndexProvider indexProvider = IndexProvider.EMPTY;
        private Monitors monitors = new Monitors();
        private LockService lockService = new ReentrantLockService();

        public Builder( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout )
        {
            this.fs = fs;
            this.pageCache = pageCache;
            this.databaseLayout = databaseLayout;
        }

        public Builder transactionApplierTransformer(
                Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer )
        {
            this.transactionApplierTransformer = transactionApplierTransformer;
            return this;
        }

        public Builder indexProvider( IndexProvider indexProvider )
        {
            this.indexProvider = indexProvider;
            return this;
        }

        public Builder databaseHealth( DatabaseHealth databaseHealth )
        {
            this.databaseHealth = databaseHealth;
            return this;
        }

        public Builder monitors( Monitors monitors )
        {
            this.monitors = monitors;
            return this;
        }

        public Builder lockService( LockService lockService )
        {
            this.lockService = lockService;
            return this;
        }

        public RecordStorageEngine build()
        {
            return get( fs, pageCache, indexProvider, databaseHealth, databaseLayout,
                    transactionApplierTransformer, monitors, lockService );
        }
    }

    private static class ExtendedRecordStorageEngine extends RecordStorageEngine
    {
        private final Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade>
                transactionApplierTransformer;

        ExtendedRecordStorageEngine( DatabaseLayout databaseLayout, Config config, PageCache pageCache, FileSystemAbstraction fs,
                LogProvider logProvider, LogProvider userLogProvider, TokenHolders tokenHolders, SchemaState schemaState,
                ConstraintSemantics constraintSemantics, JobScheduler scheduler, TokenNameLookup tokenNameLookup,
                LockService lockService, IndexProviderMap indexProviderMap,
                IndexingService.Monitor indexingServiceMonitor, DatabaseHealth databaseHealth,
                ExplicitIndexProvider explicitIndexProviderLookup,
                IndexConfigStore indexConfigStore, IdOrderingQueue explicitIndexTransactionOrdering,
                IdGeneratorFactory idGeneratorFactory, IdController idController,
                Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer, Monitors monitors,
                RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, OperationalMode operationalMode )
        {
            super( databaseLayout, config, pageCache, fs, logProvider, userLogProvider, tokenHolders, schemaState, constraintSemantics, scheduler,
                    tokenNameLookup, lockService, indexProviderMap, indexingServiceMonitor, databaseHealth, explicitIndexProviderLookup, indexConfigStore,
                    explicitIndexTransactionOrdering, idGeneratorFactory, idController, monitors, recoveryCleanupWorkCollector, operationalMode,
                    EmptyVersionContextSupplier.EMPTY );
            this.transactionApplierTransformer = transactionApplierTransformer;
        }

        @Override
        protected BatchTransactionApplierFacade applier( TransactionApplicationMode mode, IndexActivator indexActivator )
        {
            BatchTransactionApplierFacade recordEngineApplier = super.applier( mode, indexActivator );
            return transactionApplierTransformer.apply( recordEngineApplier );
        }
    }
}
