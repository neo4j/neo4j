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

import java.io.File;
import java.util.function.Function;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFileCreationMonitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

public class NeoStoreDataSourceRule extends ExternalResource
{
    private NeoStoreDataSource dataSource;

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs, PageCache pageCache )
    {
        return getDataSource( storeDir, fs, pageCache, new Dependencies() );
    }

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs, PageCache pageCache,
            DependencyResolver otherCustomOverriddenDependencies )
    {
        shutdownAnyRunning();

        StatementLocksFactory locksFactory = mock( StatementLocksFactory.class );
        StatementLocks statementLocks = mock( StatementLocks.class );
        Locks.Client locks = mock( Locks.Client.class );
        when( statementLocks.optimistic() ).thenReturn( locks );
        when( statementLocks.pessimistic() ).thenReturn( locks );
        when( locksFactory.newInstance() ).thenReturn( statementLocks );

        JobScheduler jobScheduler = mock( JobScheduler.class, RETURNS_MOCKS );
        Monitors monitors = new Monitors();

        Dependencies mutableDependencies = new Dependencies( otherCustomOverriddenDependencies );
        Config config = dependency( mutableDependencies, Config.class, deps -> Config.defaults() );
        LogService logService = dependency( mutableDependencies, LogService.class,
                deps -> new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() ) );
        IdGeneratorFactory idGeneratorFactory = dependency( mutableDependencies, IdGeneratorFactory.class,
                deps -> new DefaultIdGeneratorFactory( fs ) );
        IdTypeConfigurationProvider idConfigurationProvider = dependency( mutableDependencies,
                IdTypeConfigurationProvider.class, deps -> new CommunityIdTypeConfigurationProvider() );
        DatabaseHealth databaseHealth = dependency( mutableDependencies, DatabaseHealth.class,
                deps -> new DatabaseHealth( mock( DatabasePanicEventGenerator.class ),
                        NullLogProvider.getInstance().getLog( DatabaseHealth.class ) ) );
        SystemNanoClock clock = dependency( mutableDependencies, SystemNanoClock.class, deps -> Clocks.nanoClock() );
        TransactionMonitor transactionMonitor =
                dependency( mutableDependencies, TransactionMonitor.class, deps -> new TransactionStats() );
        AvailabilityGuard availabilityGuard = dependency( mutableDependencies, AvailabilityGuard.class,
                deps -> new AvailabilityGuard( deps.resolveDependency( SystemNanoClock.class ),
                        NullLog.getInstance() ) );

        dataSource = new NeoStoreDataSource( storeDir, config, idGeneratorFactory,
                logService, mock( JobScheduler.class, RETURNS_MOCKS ), mock( TokenNameLookup.class ),
                dependencyResolverForNoIndexProvider(), mock( PropertyKeyTokenHolder.class ),
                mock( LabelTokenHolder.class ), mock( RelationshipTypeTokenHolder.class ), locksFactory,
                mock( SchemaWriteGuard.class ), mock( TransactionEventHandlers.class ), IndexingService.NO_MONITOR,
                fs, transactionMonitor, databaseHealth,
                mock( LogFileCreationMonitor.class ), TransactionHeaderInformationFactory.DEFAULT,
                new StartupStatisticsProvider(), new CommunityCommitProcessFactory(), mock( InternalAutoIndexing.class ), pageCache,
                new StandardConstraintSemantics(), monitors,
                new Tracers( "null", NullLog.getInstance(), monitors, jobScheduler, clock ),
                mock( Procedures.class ),
                IOLimiter.unlimited(),
                availabilityGuard, clock,
                new CanWrite(), new StoreCopyCheckPointMutex(),
                RecoveryCleanupWorkCollector.immediate(),
                new BufferedIdController(
                        new BufferingIdGeneratorFactory( idGeneratorFactory, IdReuseEligibility.ALWAYS, idConfigurationProvider ), jobScheduler ),
                OperationalMode.single, new TransactionVersionContextSupplier(), ON_HEAP );
        return dataSource;
    }

    private <T> T dependency( Dependencies dependencies, Class<T> type,
            Function<DependencyResolver,T> defaultSupplier )
    {
        try
        {
            return dependencies.resolveDependency( type );
        }
        catch ( IllegalArgumentException | UnsatisfiedDependencyException e )
        {
            return dependencies.satisfyDependency( defaultSupplier.apply( dependencies ) );
        }
    }

    private void shutdownAnyRunning()
    {
        if ( dataSource != null )
        {
            dataSource.stop();
            dataSource.shutdown();
        }
    }

    @Override
    protected void after( boolean successful )
    {
        shutdownAnyRunning();
    }

    private DependencyResolver dependencyResolverForNoIndexProvider()
    {
        return new DependencyResolver.Adapter()
        {
            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
            {
                if ( IndexProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( IndexProvider.EMPTY );
                }
                throw new IllegalArgumentException( type.toString() );
            }
        };
    }
}
