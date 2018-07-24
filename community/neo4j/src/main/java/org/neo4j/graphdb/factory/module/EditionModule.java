/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.factory.module;

import java.io.File;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dmbs.database.DefaultDatabaseManager;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.watcher.RestartableFileSystemWatcher;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.ProcedureConfig;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.DefaultIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.impl.util.watcher.DefaultFileDeletionEventListener;
import org.neo4j.kernel.impl.util.watcher.DefaultFileSystemWatcherService;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;
import org.neo4j.util.FeatureToggles;

import static org.neo4j.kernel.impl.proc.temporal.TemporalFunction.registerTemporalFunctions;

/**
 * Edition module for {@link GraphDatabaseFacadeFactory}. Implementations of this class
 * need to create all the services that would be specific for a particular edition of the database.
 */
public abstract class EditionModule
{
    // This resided in RecordStorageEngine prior to 3.3
    private static final boolean safeIdBuffering = FeatureToggles.flag(
            EditionModule.class, "safeIdBuffering", true );

    public IdGeneratorFactory idGeneratorFactory;

    public IdTypeConfigurationProvider idTypeConfigurationProvider;

    public Supplier<TokenHolders> tokenHoldersSupplier;

    public Locks lockManager;

    public StatementLocksFactory statementLocksFactory;

    public CommitProcessFactory commitProcessFactory;

    public long transactionStartTimeout;

    public TransactionHeaderInformationFactory headerInformationFactory;

    public SchemaWriteGuard schemaWriteGuard;

    public ConstraintSemantics constraintSemantics;

    public CoreAPIAvailabilityGuard coreAPIAvailabilityGuard;

    public AccessCapability accessCapability;

    public IOLimiter ioLimiter;

    public IdReuseEligibility eligibleForIdReuse;

    public Function<File, FileSystemWatcherService> watcherServiceFactory;

    public IdController idController;

    public AuthManager authManager;

    public UserManagerSupplier userManagerSupplier;

    public NetworkConnectionTracker connectionTracker;

    protected FileSystemWatcherService createFileSystemWatcherService( FileSystemAbstraction fileSystem, File databaseDirectory,
            LogService logging, JobScheduler jobScheduler, Predicate<String> fileNameFilter )
    {
        try
        {
            RestartableFileSystemWatcher watcher = new RestartableFileSystemWatcher( fileSystem.fileWatcher() );
            watcher.addFileWatchEventListener( new DefaultFileDeletionEventListener( logging, fileNameFilter ) );
            watcher.watch( databaseDirectory );
            // register to watch database dir parent folder to see when database dir removed
            watcher.watch( databaseDirectory.getParentFile() );
            return new DefaultFileSystemWatcherService( jobScheduler, watcher );
        }
        catch ( Exception e )
        {
            Log log = logging.getInternalLog( getClass() );
            log.warn( "Can not create file watcher for current file system. File monitoring capabilities for store " +
                    "files will be disabled.", e );
            return FileSystemWatcherService.EMPTY_WATCHER;
        }
    }

    public void registerProcedures( Procedures procedures, ProcedureConfig procedureConfig ) throws KernelException
    {
        procedures.registerProcedure( org.neo4j.kernel.builtinprocs.BuiltInProcedures.class );
        procedures.registerProcedure( org.neo4j.kernel.builtinprocs.TokenProcedures.class );
        procedures.registerProcedure( org.neo4j.kernel.builtinprocs.BuiltInDbmsProcedures.class );
        procedures.registerBuiltInFunctions( org.neo4j.kernel.builtinprocs.BuiltInFunctions.class );
        registerTemporalFunctions( procedures, procedureConfig );

        registerEditionSpecificProcedures( procedures );
    }

    protected abstract void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException;

    protected void publishEditionInfo( UsageData sysInfo, DatabaseInfo databaseInfo, Config config )
    {
        sysInfo.set( UsageDataKeys.edition, databaseInfo.edition );
        sysInfo.set( UsageDataKeys.operationalMode, databaseInfo.operationalMode );
        config.augment( GraphDatabaseSettings.editionName, databaseInfo.edition.toString() );
    }

    public abstract void setupSecurityModule( PlatformModule platformModule, Procedures procedures );

    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, PlatformModule platform, EditionModule edition,
            Procedures procedures, Logger msgLog )
    {
        return new DefaultDatabaseManager( platform, edition, procedures, msgLog, graphDatabaseFacade );
    }

    protected static SecurityModule setupSecurityModule( PlatformModule platformModule, Log log, Procedures procedures,
            String key )
    {
        for ( SecurityModule candidate : Service.load( SecurityModule.class ) )
        {
            if ( candidate.matches( key ) )
            {
                try
                {
                    candidate.setup( new SecurityModule.Dependencies()
                    {
                        @Override
                        public LogService logService()
                        {
                            return platformModule.logging;
                        }

                        @Override
                        public Config config()
                        {
                            return platformModule.config;
                        }

                        @Override
                        public Procedures procedures()
                        {
                            return procedures;
                        }

                        @Override
                        public JobScheduler scheduler()
                        {
                            return platformModule.jobScheduler;
                        }

                        @Override
                        public FileSystemAbstraction fileSystem()
                        {
                            return platformModule.fileSystem;
                        }

                        @Override
                        public DependencySatisfier dependencySatisfier()
                        {
                            return platformModule.dependencies;
                        }
                    } );
                    return candidate;
                }
                catch ( Exception e )
                {
                    String errorMessage = "Failed to load security module.";
                    log.error( errorMessage );
                    throw new RuntimeException( errorMessage, e );
                }
            }
        }
        String errorMessage = "Failed to load security module with key '" + key + "'.";
        log.error( errorMessage );
        throw new IllegalArgumentException( errorMessage );
    }

    protected NetworkConnectionTracker createConnectionTracker()
    {
        return NetworkConnectionTracker.NO_OP;
    }

    protected void createIdComponents( PlatformModule platformModule, Dependencies dependencies, IdGeneratorFactory
            editionIdGeneratorFactory )
    {
        IdGeneratorFactory factory = editionIdGeneratorFactory;
        if ( safeIdBuffering )
        {
            BufferingIdGeneratorFactory bufferingIdGeneratorFactory =
                    new BufferingIdGeneratorFactory( factory, eligibleForIdReuse, idTypeConfigurationProvider );
            idController = createBufferedIdController( bufferingIdGeneratorFactory, platformModule.jobScheduler );
            factory = bufferingIdGeneratorFactory;
        }
        else
        {
            idController = createDefaultIdController();
        }
        this.idGeneratorFactory = factory;
    }

    private BufferedIdController createBufferedIdController( BufferingIdGeneratorFactory idGeneratorFactory,
            JobScheduler scheduler )
    {
        return new BufferedIdController( idGeneratorFactory, scheduler );
    }

    protected DefaultIdController createDefaultIdController()
    {
        return new DefaultIdController();
    }
}
