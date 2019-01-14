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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.time.Clock;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.DelegatingLabelTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingPropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.DelegatingRelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.DefaultKernelData;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.udc.UsageData;

/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule extends EditionModule
{
    public static final String COMMUNITY_SECURITY_MODULE_ID = "community-security-module";

    public CommunityEditionModule( PlatformModule platformModule )
    {
        org.neo4j.kernel.impl.util.Dependencies dependencies = platformModule.dependencies;
        Config config = platformModule.config;
        LogService logging = platformModule.logging;
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        PageCache pageCache = platformModule.pageCache;
        File storeDir = platformModule.storeDir;
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        life.add( platformModule.dataSourceManager );

        watcherService = createFileSystemWatcherService( fileSystem, storeDir, logging,
                platformModule.jobScheduler, config, fileWatcherFileNameFilter() );
        dependencies.satisfyDependencies( watcherService );
        life.add( watcherService );

        this.accessCapability = config.get( GraphDatabaseSettings.read_only ) ? new ReadOnly() : new CanWrite();

        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        dependencies.satisfyDependency( SslPolicyLoader.create( config, logging.getInternalLogProvider() ) ); // for bolt and web server

        lockManager = dependencies.satisfyDependency( createLockManager( config, platformModule.clock, logging ) );
        statementLocksFactory = createStatementLocksFactory( lockManager, config, logging );

        idTypeConfigurationProvider = createIdTypeConfigurationProvider( config );
        eligibleForIdReuse = IdReuseEligibility.ALWAYS;

        createIdComponents( platformModule, dependencies, createIdGeneratorFactory( fileSystem, idTypeConfigurationProvider ) );
        dependencies.satisfyDependency( idGeneratorFactory );
        dependencies.satisfyDependency( idController );

        propertyKeyTokenHolder = life.add( dependencies.satisfyDependency( new DelegatingPropertyKeyTokenHolder(
                createPropertyKeyCreator( config, dataSourceManager, idGeneratorFactory ) ) ) );
        labelTokenHolder = life.add( dependencies.satisfyDependency(new DelegatingLabelTokenHolder( createLabelIdCreator( config,
                dataSourceManager, idGeneratorFactory ) ) ));
        relationshipTypeTokenHolder = life.add( dependencies.satisfyDependency(new DelegatingRelationshipTypeTokenHolder(
                createRelationshipTypeCreator( config, dataSourceManager, idGeneratorFactory ) ) ));

        dependencies.satisfyDependency(
                createKernelData( fileSystem, pageCache, storeDir, config, graphDatabaseFacade, life ) );

        commitProcessFactory = new CommunityCommitProcessFactory();

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = createSchemaRuleVerifier();

        coreAPIAvailabilityGuard = new CoreAPIAvailabilityGuard( platformModule.availabilityGuard, transactionStartTimeout );

        ioLimiter = IOLimiter.unlimited();

        registerRecovery( platformModule.databaseInfo, life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );

        dependencies.satisfyDependency( createSessionTracker() );
    }

    protected Predicate<String> fileWatcherFileNameFilter()
    {
        return communityFileWatcherFileNameFilter();
    }

    static Predicate<String> communityFileWatcherFileNameFilter()
    {
        return Predicates.any(
                fileName -> fileName.startsWith( TransactionLogFiles.DEFAULT_NAME ),
                fileName -> fileName.startsWith( IndexConfigStore.INDEX_DB_FILE_NAME )
        );
    }

    protected IdTypeConfigurationProvider createIdTypeConfigurationProvider( Config config )
    {
        return new CommunityIdTypeConfigurationProvider();
    }

    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new StandardConstraintSemantics();
    }

    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new SimpleStatementLocksFactory( locks );
    }

    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return SchemaWriteGuard.ALLOW_ALL_WRITES;
    }

    private TokenCreator createRelationshipTypeCreator( Config config, DataSourceManager dataSourceManager,
            IdGeneratorFactory idGeneratorFactory )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultRelationshipTypeCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    private TokenCreator createPropertyKeyCreator( Config config, DataSourceManager dataSourceManager,
            IdGeneratorFactory idGeneratorFactory )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultPropertyTokenCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    private TokenCreator createLabelIdCreator( Config config, DataSourceManager dataSourceManager,
            IdGeneratorFactory idGeneratorFactory )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultLabelIdCreator( dataSourceManager, idGeneratorFactory );
        }
    }

    private KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
            Config config, GraphDatabaseAPI graphAPI, LifeSupport life )
    {
        return life.add( new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphAPI ) );
    }

    protected IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fs,
            IdTypeConfigurationProvider idTypeConfigurationProvider )
    {
        return new DefaultIdGeneratorFactory( fs, idTypeConfigurationProvider );
    }

    public static Locks createLockManager( Config config, Clock clock, LogService logging )
    {
        String key = config.get( GraphDatabaseFacadeFactory.Configuration.lock_manager );
        for ( Locks.Factory candidate : Service.load( Locks.Factory.class ) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( key.equals( candidateId ) )
            {
                return candidate.newInstance( config, clock, ResourceTypes.values() );
            }
            else if ( key.equals( "" ) )
            {
                logging.getInternalLog( CommunityEditionModule.class )
                        .info( "No locking implementation specified, defaulting to '" + candidateId + "'" );
                return candidate.newInstance( config, clock, ResourceTypes.values() );
            }
        }

        if ( key.equals( "community" ) )
        {
            return new CommunityLockManger( config, clock );
        }
        else if ( key.equals( "" ) )
        {
            logging.getInternalLog( CommunityEditionModule.class )
                    .info( "No locking implementation specified, defaulting to 'community'" );
            return new CommunityLockManger( config, clock );
        }

        throw new IllegalArgumentException( "No lock manager found with the name '" + key + "'." );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    private void registerRecovery( final DatabaseInfo databaseInfo, LifeSupport life,
            final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( ( instance, from, to ) ->
        {
            if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
            {
                doAfterRecoveryAndStartup( databaseInfo, dependencyResolver );
            }
        } );
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        // Community does not add any extra procedures
    }

    @Override
    public void setupSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            setupSecurityModule( platformModule, platformModule.logging.getUserLog( getClass() ),
                    procedures, COMMUNITY_SECURITY_MODULE_ID );
        }
        else
        {
            platformModule.life.add( platformModule.dependencies.satisfyDependency( AuthManager.NO_AUTH ) );
            platformModule.life.add( platformModule.dependencies.satisfyDependency( UserManagerSupplier.NO_AUTH ) );
        }
    }
}
