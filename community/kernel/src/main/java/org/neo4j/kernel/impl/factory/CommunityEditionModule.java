/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.factory;

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.DatabaseAvailability;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdTypeConfigurationProvider;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.index.RemoveOrphanConstraintIndexesOnStartup;
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
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.locking.community.CommunityLockManger;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;


/**
 * This implementation of {@link org.neo4j.kernel.impl.factory.EditionModule} creates the implementations of services
 * that are specific to the Community edition.
 */
public class CommunityEditionModule
    extends EditionModule
{
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
        GraphDatabaseFacade graphDatabaseFacade = platformModule.graphDatabaseFacade;

        lockManager = dependencies.satisfyDependency( createLockManager( config, logging ) );
        statementLocksFactory = createStatementLocksFactory( lockManager, config, logging );

        idTypeConfigurationProvider = createIdTypeConfigurationProvider( config );
        idGeneratorFactory = dependencies.satisfyDependency( createIdGeneratorFactory( fileSystem, idTypeConfigurationProvider ) );

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

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout );

        upgradeConfiguration = new ConfigMapUpgradeConfiguration( config );

        constraintSemantics = createSchemaRuleVerifier();

        eligibleForIdReuse = IdReuseEligibility.ALWAYS;

        registerRecovery( config.get( GraphDatabaseFacadeFactory.Configuration.editionName), life, dependencies );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ) );
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

    private void publishEditionInfo( UsageData sysInfo )
    {
        sysInfo.set( UsageDataKeys.edition, determineEdition() );
        sysInfo.set( UsageDataKeys.operationalMode, UsageDataKeys.OperationalMode.single );
    }

    private UsageDataKeys.Edition determineEdition()
    {
        // Currently, a user can be running enterprise or advanced edition and end up using this module to bootstrap
        // So, until we've organized this differently, we use introspection to tell which edition is running
        try
        {
            getClass().getClassLoader().loadClass( "org.neo4j.kernel.ha.HighlyAvailableGraphDatabase" );
            return UsageDataKeys.Edition.enterprise;
        }
        catch ( ClassNotFoundException e )
        {
            // Not Enterprise
        }
        try
        {
            getClass().getClassLoader().loadClass( "org.neo4j.management.Neo4jManager" );
            return UsageDataKeys.Edition.advanced;
        }
        catch ( ClassNotFoundException e )
        {
            // Not Advanced
        }
        return UsageDataKeys.Edition.community;

    }

    protected SchemaWriteGuard createSchemaWriteGuard()
    {
        return new SchemaWriteGuard()
        {
            @Override
            public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
            {
            }
        };
    }


    protected TokenCreator createRelationshipTypeCreator( Config config, DataSourceManager dataSourceManager,
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

    protected TokenCreator createPropertyKeyCreator( Config config, DataSourceManager dataSourceManager,
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

    protected TokenCreator createLabelIdCreator( Config config, DataSourceManager dataSourceManager,
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

    protected KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
            Config config, GraphDatabaseAPI graphAPI, LifeSupport life )
    {
        return life.add( new DefaultKernelData( fileSystem, pageCache, storeDir, config, graphAPI ) );
    }

    protected IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fs, IdTypeConfigurationProvider idTypeConfigurationProvider )
    {
        return new DefaultIdGeneratorFactory( fs, idTypeConfigurationProvider );
    }

    public static Locks createLockManager( Config config, LogService logging )
    {
        String key = config.get( GraphDatabaseFacadeFactory.Configuration.lock_manager );
        for ( Locks.Factory candidate : Service.load( Locks.Factory.class ) )
        {
            String candidateId = candidate.getKeys().iterator().next();
            if ( candidateId.equals( key ) )
            {
                return candidate.newInstance( ResourceTypes.values() );
            }
            else if ( key.equals( "" ) )
            {
                logging.getInternalLog( CommunityFacadeFactory.class )
                        .info( "No locking implementation specified, defaulting to '" + candidateId + "'" );
                return candidate.newInstance( ResourceTypes.values() );
            }
        }

        if ( key.equals( "community" ) )
        {
            return new CommunityLockManger();
        }
        else if ( key.equals( "" ) )
        {
            logging.getInternalLog( CommunityFacadeFactory.class )
                    .info( "No locking implementation specified, defaulting to 'community'" );
            return new CommunityLockManger();
        }

        throw new IllegalArgumentException( "No lock manager found with the name '" + key + "'." );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    protected void registerRecovery( final String editionName, LifeSupport life,
                                     final DependencyResolver dependencyResolver )
    {
        life.addLifecycleListener( new LifecycleListener()
        {
            @Override
            public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
            {
                if ( instance instanceof DatabaseAvailability && to.equals( LifecycleStatus.STARTED ) )
                {
                    doAfterRecoveryAndStartup( editionName, dependencyResolver );
                }
            }
        } );
    }

    @Override
    protected void doAfterRecoveryAndStartup( String editionName, DependencyResolver dependencyResolver )
    {
        super.doAfterRecoveryAndStartup( editionName, dependencyResolver );

        new RemoveOrphanConstraintIndexesOnStartup( dependencyResolver.resolveDependency( NeoStoreDataSource.class )
                .getKernel(), dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider() ).perform();
    }

    protected final class DefaultKernelData extends KernelData implements Lifecycle
    {
        private final GraphDatabaseAPI graphDb;

        public DefaultKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config,
                GraphDatabaseAPI graphDb )
        {
            super( fileSystem, pageCache, storeDir, config );
            this.graphDb = graphDb;
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return graphDb;
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void stop() throws Throwable
        {
        }
    }
}
