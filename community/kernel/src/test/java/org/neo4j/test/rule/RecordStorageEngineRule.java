/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.test.rule;

import java.io.File;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.BatchTransactionApplierFacade;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.impl.EphemeralIdGenerator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.test.rule.NeoStoreDataSourceRule.nativeLabelScanStoreProvider;

/**
 * Conveniently manages a {@link RecordStorageEngine} in a test. Needs {@link FileSystemAbstraction} and
 * {@link PageCache}, which usually are managed by test rules themselves. That's why they are passed in
 * when {@link #getWith(FileSystemAbstraction, PageCache) getting (constructing)} the engine. Further
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

    public Builder getWith( FileSystemAbstraction fs, PageCache pageCache )
    {
        return new Builder( fs, pageCache );
    }

    private RecordStorageEngine get( FileSystemAbstraction fs, PageCache pageCache,
            SchemaIndexProvider schemaIndexProvider, DatabaseHealth databaseHealth, File storeDirectory,
            Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer,
            Monitors monitors )
    {
        if ( !fs.fileExists( storeDirectory ) && !fs.mkdir( storeDirectory ) )
        {
            throw new IllegalStateException();
        }
        IdGeneratorFactory idGeneratorFactory = new EphemeralIdGenerator.Factory();
        LabelScanStoreProvider labelScanStoreProvider =
                nativeLabelScanStoreProvider( storeDirectory, fs, pageCache, monitors );
        LegacyIndexProviderLookup legacyIndexProviderLookup = mock( LegacyIndexProviderLookup.class );
        when( legacyIndexProviderLookup.all() ).thenReturn( Iterables.empty() );
        IndexConfigStore indexConfigStore = new IndexConfigStore( storeDirectory, fs );
        JobScheduler scheduler = life.add( new Neo4jJobScheduler() );
        Config config = Config.defaults();
        Supplier<KernelTransactionsSnapshot> txSnapshotSupplier =
                () -> new KernelTransactionsSnapshot( Collections.emptySet(), 0 );
        return life.add( new ExtendedRecordStorageEngine( storeDirectory, config, idGeneratorFactory,
                IdReuseEligibility.ALWAYS, new CommunityIdTypeConfigurationProvider(), pageCache, fs,
                NullLogProvider.getInstance(),
                mock( PropertyKeyTokenHolder.class ), mock( LabelTokenHolder.class ),
                mock( RelationshipTypeTokenHolder.class ), () -> {}, new StandardConstraintSemantics(),
                scheduler, mock( TokenNameLookup.class ), new ReentrantLockService(),
                schemaIndexProvider, IndexingService.NO_MONITOR, databaseHealth,
                labelScanStoreProvider, legacyIndexProviderLookup, indexConfigStore,
                new SynchronizedArrayIdOrderingQueue( 20 ), txSnapshotSupplier, transactionApplierTransformer ) );
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
        private File storeDirectory = new File( "/graph.db" );
        private Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer =
                applierFacade -> applierFacade;
        private SchemaIndexProvider schemaIndexProvider = SchemaIndexProvider.NO_INDEX_PROVIDER;
        private Monitors monitors = new Monitors();

        public Builder( FileSystemAbstraction fs, PageCache pageCache )
        {
            this.fs = fs;
            this.pageCache = pageCache;
        }

        public Builder transactionApplierTransformer(
                Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade> transactionApplierTransformer )
        {
            this.transactionApplierTransformer = transactionApplierTransformer;
            return this;
        }

        public Builder indexProvider( SchemaIndexProvider schemaIndexProvider )
        {
            this.schemaIndexProvider = schemaIndexProvider;
            return this;
        }

        public Builder databaseHealth( DatabaseHealth databaseHealth )
        {
            this.databaseHealth = databaseHealth;
            return this;
        }

        public Builder storeDirectory( File storeDirectory )
        {
            this.storeDirectory = storeDirectory;
            return this;
        }

        public Builder monitors( Monitors monitors )
        {
            this.monitors = monitors;
            return this;
        }

        // Add more here

        public RecordStorageEngine build()
        {
            return get( fs, pageCache, schemaIndexProvider, databaseHealth, storeDirectory,
                    transactionApplierTransformer, monitors );
        }
    }

    private class ExtendedRecordStorageEngine extends RecordStorageEngine
    {

        private final Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade>
                transactionApplierTransformer;

        ExtendedRecordStorageEngine( File storeDir, Config config,
                IdGeneratorFactory idGeneratorFactory, IdReuseEligibility eligibleForReuse,
                IdTypeConfigurationProvider idTypeConfigurationProvider,
                PageCache pageCache, FileSystemAbstraction fs, LogProvider logProvider,
                PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokens,
                RelationshipTypeTokenHolder relationshipTypeTokens, Runnable schemaStateChangeCallback,
                ConstraintSemantics constraintSemantics, JobScheduler scheduler,
                TokenNameLookup tokenNameLookup, LockService lockService,
                SchemaIndexProvider indexProvider,
                IndexingService.Monitor indexingServiceMonitor, DatabaseHealth databaseHealth,
                LabelScanStoreProvider labelScanStoreProvider,
                LegacyIndexProviderLookup legacyIndexProviderLookup,
                IndexConfigStore indexConfigStore, IdOrderingQueue legacyIndexTransactionOrdering,
                Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier,
                Function<BatchTransactionApplierFacade,BatchTransactionApplierFacade>
                        transactionApplierTransformer )
        {
            super( storeDir, config, idGeneratorFactory, eligibleForReuse, idTypeConfigurationProvider,
                    pageCache, fs, logProvider, propertyKeyTokenHolder,
                    labelTokens, relationshipTypeTokens, schemaStateChangeCallback, constraintSemantics, scheduler,
                    tokenNameLookup, lockService, indexProvider, indexingServiceMonitor, databaseHealth,
                    labelScanStoreProvider,
                    legacyIndexProviderLookup, indexConfigStore, legacyIndexTransactionOrdering,
                    transactionsSnapshotSupplier );
            this.transactionApplierTransformer = transactionApplierTransformer;
        }

        @Override
        protected BatchTransactionApplierFacade applier( TransactionApplicationMode mode )
        {
            BatchTransactionApplierFacade recordEngineApplier = super.applier( mode );
            return transactionApplierTransformer.apply( recordEngineApplier );
        }
    }
}
