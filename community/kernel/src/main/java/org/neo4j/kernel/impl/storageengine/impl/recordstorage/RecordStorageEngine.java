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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.LegacyIndexProviderLookup;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.cache.BridgingCacheAccess;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.storageengine.StorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.TRUE;
import static org.neo4j.helpers.Settings.setting;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;

public class RecordStorageEngine implements StorageEngine, Lifecycle
{
    /**
     * This setting is hidden to the user and is here merely for making it easier to back out of
     * a change where reading property chains incurs read locks on {@link LockService}.
     */
    private static final Setting<Boolean> use_read_locks_on_property_reads =
            setting( "experimental.use_read_locks_on_property_reads", BOOLEAN, TRUE );

    private final StoreReadLayer storeLayer;
    private final IndexingService indexingService;
    private final NeoStores neoStores;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final KernelHealth kernelHealth;
    private final IndexConfigStore indexConfigStore;
    private final SchemaCache schemaCache;
    private final IntegrityValidator integrityValidator;
    private final IndexUpdatesValidator indexUpdatesValidator;
    private final CacheAccessBackDoor cacheAccess;
    private final LabelScanStore labelScanStore;
    private final DefaultSchemaIndexProviderMap providerMap;
    private final ProcedureCache procedureCache;
    private final LegacyIndexApplierLookup legacyIndexApplierLookup;

    public RecordStorageEngine(
            File storeDir,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogProvider logProvider,
            PropertyKeyTokenHolder propertyKeyTokenHolder,
            LabelTokenHolder labelTokens,
            RelationshipTypeTokenHolder relationshipTypeTokens,
            Runnable schemaStateChangeCallback,
            ConstraintSemantics constraintSemantics,
            JobScheduler scheduler,
            TokenNameLookup tokenNameLookup,
            LockService lockService,
            SchemaIndexProvider indexProvider,
            IndexingService.Monitor indexingServiceMonitor,
            KernelHealth kernelHealth,
            LabelScanStoreProvider labelScanStoreProvider,
            LegacyIndexProviderLookup legacyIndexProviderLookup,
            IndexConfigStore indexConfigStore )
    {
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokens;
        this.labelTokenHolder = labelTokens;
        this.kernelHealth = kernelHealth;
        this.indexConfigStore = indexConfigStore;
        final StoreFactory storeFactory = new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fs, logProvider );
        neoStores = storeFactory.openAllNeoStores( true );

        try
        {
            schemaCache = new SchemaCache( constraintSemantics, Collections.<SchemaRule>emptyList() );
            SchemaStorage schemaStorage = new SchemaStorage( neoStores.getSchemaStore() );

            providerMap = new DefaultSchemaIndexProviderMap( indexProvider );
            indexingService = IndexingService.create(
                    new IndexSamplingConfig( config ), scheduler, providerMap,
                    new NeoStoreIndexStoreView( lockService, neoStores ), tokenNameLookup,
                    toList( new SchemaStorage( neoStores.getSchemaStore() ).allIndexRules() ), logProvider,
                    indexingServiceMonitor, schemaStateChangeCallback );

            integrityValidator = new IntegrityValidator( neoStores, indexingService );
            indexUpdatesValidator = new OnlineIndexUpdatesValidator(
                    neoStores, kernelHealth, new PropertyLoader( neoStores ),
                    indexingService, IndexUpdateMode.ONLINE );
            cacheAccess = new BridgingCacheAccess( schemaCache, schemaStateChangeCallback,
                    propertyKeyTokenHolder, relationshipTypeTokens, labelTokens );

            DiskLayer diskLayer = new DiskLayer( propertyKeyTokenHolder, labelTokens, relationshipTypeTokens,
                    schemaStorage,
                    neoStores, indexingService, storeStatementFactory( neoStores, config, lockService ) );
            procedureCache = new ProcedureCache();
            storeLayer = new CacheLayer( diskLayer, schemaCache, procedureCache );
            this.labelScanStore = labelScanStoreProvider.getLabelScanStore();
            legacyIndexApplierLookup = new LegacyIndexApplierLookup.Direct( legacyIndexProviderLookup );
        }
        catch ( Throwable failure )
        {
            neoStores.close();
            throw failure;
        }
    }

    private static Factory<StoreStatement> storeStatementFactory(
            NeoStores neoStores, Config config, LockService lockService )
    {
        final LockService currentLockService =
                config.get( use_read_locks_on_property_reads ) ? lockService : NO_LOCK_SERVICE;
        return () -> new StoreStatement( neoStores, currentLockService );
    }

    @Override
    public StoreReadLayer storeReadLayer()
    {
        return storeLayer;
    }

    @Override
    public TransactionIdStore transactionIdStore()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public LogVersionRepository logVersionRepository()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public ProcedureCache procedureCache()
    {
        return procedureCache;
    }

    @Override
    public NeoStores neoStores()
    {
        return neoStores;
    }

    @Override
    public MetaDataStore metaDataStore()
    {
        return neoStores.getMetaDataStore();
    }

    @Override
    public IndexingService indexingService()
    {
        return indexingService;
    }

    @Override
    public IndexUpdatesValidator indexUpdatesValidator()
    {
        return indexUpdatesValidator;
    }

    @Override
    public LabelScanStore labelScanStore()
    {
        return labelScanStore;
    }

    @Override
    public IntegrityValidator integrityValidator()
    {
        return integrityValidator;
    }

    @Override
    public SchemaIndexProviderMap schemaIndexProviderMap()
    {
        return providerMap;
    }

    @Override
    public CacheAccessBackDoor cacheAccess()
    {
        return cacheAccess;
    }

    @Override
    public LegacyIndexApplierLookup legacyIndexApplierLookup()
    {
        return legacyIndexApplierLookup;
    }

    @Override
    public IndexConfigStore indexConfigStore()
    {
        return indexConfigStore;
    }

    @Override
    public KernelHealth kernelHealth()
    {
        return kernelHealth;
    }

    @Override
    public void init() throws Throwable
    {
        indexingService.init();
        labelScanStore.init();
    }

    @Override
    public void start() throws Throwable
    {
        neoStores.makeStoreOk();

        propertyKeyTokenHolder.setInitialTokens(
                neoStores.getPropertyKeyTokenStore().getTokens( Integer.MAX_VALUE ) );
        relationshipTypeTokenHolder.setInitialTokens(
                neoStores.getRelationshipTypeTokenStore().getTokens( Integer.MAX_VALUE ) );
        labelTokenHolder.setInitialTokens(
                neoStores.getLabelTokenStore().getTokens( Integer.MAX_VALUE ) );

        neoStores.rebuildCountStoreIfNeeded(); // TODO: move this to counts store lifecycle
        loadSchemaCache();
        indexingService.start();
        labelScanStore.start();
    }

    @Override
    public void loadSchemaCache()
    {
        List<SchemaRule> schemaRules = toList( neoStores.getSchemaStore().loadAllSchemaRules() );
        schemaCache.load( schemaRules );
    }

    @Override
    public void stop() throws Throwable
    {
        labelScanStore.stop();
        indexingService.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        labelScanStore.shutdown();
        indexingService.shutdown();
    }
}
