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
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.function.Consumer;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.store.StorageLayer;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.DelegatingLabelTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingPropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingRelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.enterprise.EnterpriseConstraintSemantics;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.newapi.KernelToken;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotationImpl;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreReadLayer;

class ForceCheckpointBackupService
{
    private class AlwaysTrueCheckPointThreshold implements CheckPointThreshold
    {
        @Override
        public void initialize( long transactionId )
        {
        }

        @Override
        public boolean isCheckPointingNeeded( long lastCommittedTransactionId, Consumer<String> consumer )
        {
            return true;
        }

        @Override
        public void checkPointHappened( long transactionId )
        {
        }

        @Override
        public long checkFrequencyMillis()
        {
            return 1;
        }
    }

    private final PageCache pageCache;
    private final Config config;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final LogProvider logProvider;
    private final Monitors monitors;

    ForceCheckpointBackupService( FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider, PageCache pageCache )
    {
        this.pageCache = pageCache;
        config = Config.defaults();
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.logProvider = logProvider;
        monitors = new Monitors(); //TODO
    }

    void forceCheckpoint( File backupDir )
    {
        LifeSupport lifeSupport = new LifeSupport();
        CheckPointerImpl checkPointer = getCheckPointer( backupDir, lifeSupport );
        try
        {
            checkPointer.forceCheckPoint( new SimpleTriggerInfo( "triggerName" ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    void forcePrune( LogFiles logFiles )
    {
        LogPruningImpl logPruning = getLogPruning( logFiles );
        logPruning.pruneLogs( logFiles.getHighestLogVersion() );
    }

    LogFiles getLogFiles( File backupDirectory, PageCache pageCache, FileSystemAbstraction fileSystemAbstraction )
    {
        try
        {
            return LogFilesBuilder.activeFilesBuilder( backupDirectory, fileSystemAbstraction, pageCache ).build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    void forceRotation( LogFiles logFiles, LogProvider logProvider )
    {
        LifeSupport lifeSupport = new LifeSupport();
        lifeSupport.add( logFiles );

        lifeSupport.init();
        lifeSupport.start();
        LogRotationImpl logRotation = getLogRotation( lifeSupport, logProvider, monitors, logFiles );
        try
        {
            logRotation.rotateLogFile();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            lifeSupport.stop();
            lifeSupport.shutdown();
        }
    }

    private CheckPointerImpl getCheckPointer( File backupDir, LifeSupport lifeSupport )
    {
        ReadOnlyTransactionIdStore transactionIdStore = readOnlyTransactionIdStore( pageCache, backupDir );
        CheckPointThreshold checkPointThreshold = new AlwaysTrueCheckPointThreshold();
        StorageEngine storageEngine = storageEngine( backupDir );
        LogFiles logFiles = getLogFiles( backupDir, pageCache, fileSystemAbstraction );
        LogPruning logPruning = getLogPruning( logFiles );
        LogRotation logRotation = getLogRotation( lifeSupport, logProvider, monitors, logFiles );
        int transactionCacheSize = 123; // TODO
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( transactionCacheSize );
        TransactionIdStore transactionidStore = new MetaDataStore(); //TODO must be easier to get
        DatabaseHealth databaseHealth = new DatabaseHealth();
        TransactionAppender transactionAppender =
                new BatchingTransactionAppender( logFiles, logRotation, transactionMetadataCache, transactionidStore, idOrderingQueue, databaseHealth );
        return new CheckPointerImpl( transactionIdStore, checkPointThreshold, storageEngine, logPruning, transactionAppender, databaseHealth, logProvider,
                checkPointTracer, ioLimiter, storeCopyCheckPointMutex );
    }

    private static DatabaseHealth getDatabaseHealth( LifeSupport lifeSupport, LogProvider logProvider )
    {
        KernelEventHandlers kernelEventHandlers = lifeSupport.add( new KernelEventHandlers( logProvider.getLog( KernelEventHandlers.class ) ) );
        DatabasePanicEventGenerator databasePanicEventGenerator = new DatabasePanicEventGenerator( kernelEventHandlers );
        DatabaseHealth databaseHealth = new DatabaseHealth( databasePanicEventGenerator, logProvider.getLog( DatabaseHealth.class ) );
        return databaseHealth;
    }

    private static LogRotationImpl getLogRotation( LifeSupport lifeSupport, LogProvider logProvider, Monitors monitors, LogFiles logFiles )
    {
        LogRotation.Monitor monitor = monitors.newMonitor( LogRotation.Monitor.class );
        DatabaseHealth databaseHealth = getDatabaseHealth( lifeSupport, logProvider );
        return new LogRotationImpl( monitor, logFiles, databaseHealth );
    }

    private LogPruningImpl getLogPruning( LogFiles logFiles )
    {
        LogPruneStrategyFactory logPruneStrategyFactory = new LogPruneStrategyFactory();
        Clock clock = Clock.systemDefaultZone();
        Config config = Config.defaults();
        LogPruningImpl logPruningImpl = new LogPruningImpl( fileSystemAbstraction, logFiles, logProvider, logPruneStrategyFactory, clock, config );
        return logPruningImpl;
    }

    private StorageEngine storageEngine( File backupDir, LifeSupport lifeSupport )
    {
        PropertyKeyTokenHolder propertyKeyTokenHolder = new DelegatingPropertyKeyTokenHolder( new ReadOnlyTokenCreator() );
        LabelTokenHolder labelTokenHolder = new DelegatingLabelTokenHolder( new ReadOnlyTokenCreator() );
        RelationshipTypeTokenHolder relationshipTypeTokenHolder = new DelegatingRelationshipTypeTokenHolder( new ReadOnlyTokenCreator() );
        SchemaState schemaState = new DatabaseSchemaState( logProvider );
        ConstraintSemantics constraintSemantics = new EnterpriseConstraintSemantics();
        JobScheduler jobScheduler = lifeSupport.add( new CentralJobScheduler() );
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystemAbstraction );
        VersionContextSupplier versionContextSupplier = new TransactionVersionContextSupplier();
        NeoStores neoStores = new StoreFactory( backupDir, config, idGeneratorFactory, pageCache, fileSystemAbstraction, logProvider,
                versionContextSupplier ).openNeoStores();
        SchemaStorage schemaStorage = new SchemaStorage( neoStores.getSchemaStore() );
        IndexProvider indexProvider = IndexProvider.EMPTY;
        IndexProviderMap indexProviderMap = new DefaultIndexProviderMap( indexProvider );
        LockService lockService = LockService.NO_LOCK_SERVICE;
        IndexStoreView indexStoreView = new NeoStoreIndexStoreView( lockService, neoStores );
        StoreReadLayer storeReadLayer =
                new StorageLayer( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder, schemaStorage, neoStores, indexingService,
                        storeStatementSupplier, schemaCache );
        TokenRead tokenRead = new KernelToken( storeReadLayer, kernelTransactionImplementation );
        TokenNameLookup tokenNameLookup = new SilentTokenNameLookup( tokenRead );
        IndexingService indexingService =
                IndexingServiceFactory.createIndexingService( config, jobScheduler, indexProviderMap, indexStoreView, tokenNameLookup, indexRules, logProvider,
                        monitors, schemaState );
        return new RecordStorageEngine( backupDir, config, pageCache, fileSystemAbstraction, logProvider, propertyKeyTokenHolder, labelTokenHolder,
                relationshipTypeTokenHolder, schemaState, constraintSemantics, jobScheduler, tokenNameLookup, lockService, indexProviderMap,
                indexServiceMonitor, databaseHealth, explicitIndexProviderLookup, indexConfigStore, idOrderingQueue, idGeneratorFactory, idController, monitors,
                recoveryCleanupWorkCcollector, operationalMode, versionContextSupplier );
    }

    private static ReadOnlyTransactionIdStore readOnlyTransactionIdStore( PageCache pageCache, File backupDir )
    {
        try
        {
            return new ReadOnlyTransactionIdStore( pageCache, backupDir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
