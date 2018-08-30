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
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.ReadOnly;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.udc.UsageData;

import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockManager;

/**
 * This implementation of {@link EditionModule} creates the implementations of services
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
        DataSourceManager dataSourceManager = platformModule.dataSourceManager;
        LifeSupport life = platformModule.life;
        life.add( platformModule.dataSourceManager );

        watcherServiceFactory = databaseDir -> createFileSystemWatcherService( fileSystem, databaseDir, logging,
                platformModule.jobScheduler, config, fileWatcherFileNameFilter() );

        this.accessCapability = config.get( GraphDatabaseSettings.read_only ) ? new ReadOnly() : new CanWrite();

        dependencies.satisfyDependency(
                SslPolicyLoader.create( config, logging.getInternalLogProvider() ) ); // for bolt and web server

        LocksFactory lockFactory = createLockFactory( config, logging );
        locksSupplier = () -> createLockManager( lockFactory, config, platformModule.clock );
        statementLocksFactoryProvider = locks -> createStatementLocksFactory( locks, config, logging );

        threadToTransactionBridge = dependencies.satisfyDependency(
                new ThreadToStatementContextBridge( getGlobalAvailabilityGuard( platformModule.clock, logging, platformModule.config ) ) );

        idContextFactory = createIdContextFactory( platformModule, fileSystem );

        tokenHoldersSupplier = () -> new TokenHolders(
                new DelegatingTokenHolder( createPropertyKeyCreator( config, dataSourceManager ), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( createLabelIdCreator( config, dataSourceManager ), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( createRelationshipTypeCreator( config, dataSourceManager ), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );

        File kernelContextDirectory = platformModule.storeLayout.storeDirectory();
        dependencies.satisfyDependency( createKernelData( fileSystem, pageCache, kernelContextDirectory, config, life, dataSourceManager ) );

        commitProcessFactory = new CommunityCommitProcessFactory();

        headerInformationFactory = createHeaderInformationFactory();

        schemaWriteGuard = createSchemaWriteGuard();

        transactionStartTimeout = config.get( GraphDatabaseSettings.transaction_start_timeout ).toMillis();

        constraintSemantics = createSchemaRuleVerifier();

        ioLimiter = IOLimiter.UNLIMITED;

        connectionTracker = dependencies.satisfyDependency( createConnectionTracker() );

        publishEditionInfo( dependencies.resolveDependency( UsageData.class ), platformModule.databaseInfo, config );
    }

    protected IdContextFactory createIdContextFactory( PlatformModule platformModule, FileSystemAbstraction fileSystem )
    {
        return IdContextFactoryBuilder.of( fileSystem, platformModule.jobScheduler ).build();
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

    private TokenCreator createRelationshipTypeCreator( Config config, DataSourceManager dataSourceManager )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultRelationshipTypeCreator( dataSourceManager );
        }
    }

    private TokenCreator createPropertyKeyCreator( Config config, DataSourceManager dataSourceManager )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultPropertyTokenCreator( dataSourceManager );
        }
    }

    private TokenCreator createLabelIdCreator( Config config, DataSourceManager dataSourceManager )
    {
        if ( config.get( GraphDatabaseSettings.read_only ) )
        {
            return new ReadOnlyTokenCreator();
        }
        else
        {
            return new DefaultLabelIdCreator( dataSourceManager );
        }
    }

    private KernelData createKernelData( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir,
            Config config, LifeSupport life, DataSourceManager dataSourceManager )
    {
        return life.add( new KernelData( fileSystem, pageCache, storeDir, config, dataSourceManager ) );
    }

    protected TransactionHeaderInformationFactory createHeaderInformationFactory()
    {
        return TransactionHeaderInformationFactory.DEFAULT;
    }

    @Override
    public void registerEditionSpecificProcedures( Procedures procedures ) throws KernelException
    {
        // Community does not add any extra procedures
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( platformModule,
                    platformModule.logging.getUserLog( getClass() ),
                    procedures, COMMUNITY_SECURITY_MODULE_ID );
            this.authManager = securityModule.authManager();
            this.userManagerSupplier = securityModule.userManagerSupplier();
            platformModule.life.add( securityModule );
        }
        else
        {
            this.authManager = AuthManager.NO_AUTH;
            this.userManagerSupplier = UserManagerSupplier.NO_AUTH;
            platformModule.life.add( platformModule.dependencies.satisfyDependency( this.authManager ) );
            platformModule.life.add( platformModule.dependencies.satisfyDependency( this.userManagerSupplier ) );
        }
    }
}
