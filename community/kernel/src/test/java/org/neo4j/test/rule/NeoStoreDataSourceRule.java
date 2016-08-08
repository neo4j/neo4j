/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.StartupStatisticsProvider;
import org.neo4j.kernel.impl.factory.CommunityCommitProcessFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.TransactionEventHandlers;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class NeoStoreDataSourceRule extends ExternalResource
{
    private NeoStoreDataSource dataSource;

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs,
            PageCache pageCache, Map<String,String> additionalConfig, DatabaseHealth databaseHealth )
    {
        CommunityIdTypeConfigurationProvider idTypeConfigurationProvider =
                new CommunityIdTypeConfigurationProvider();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        NullLogService logService = NullLogService.getInstance();
        return getDataSource( storeDir, fs, idGeneratorFactory, idTypeConfigurationProvider, pageCache,
                additionalConfig, databaseHealth, logService );
    }

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs,
            IdGeneratorFactory idGeneratorFactory, IdTypeConfigurationProvider idConfigurationProvider,
            PageCache pageCache, Map<String, String> additionalConfig, DatabaseHealth databaseHealth,
            LogService logService )
    {
        if ( dataSource != null )
        {
            dataSource.stop();
            dataSource.shutdown();
        }
        final Config config = new Config( stringMap( additionalConfig ), GraphDatabaseSettings.class );

        StatementLocksFactory locksFactory = mock( StatementLocksFactory.class );
        StatementLocks statementLocks = mock( StatementLocks.class );
        Locks.Client locks = mock( Locks.Client.class );
        when( statementLocks.optimistic() ).thenReturn( locks );
        when( statementLocks.pessimistic() ).thenReturn( locks );
        when( locksFactory.newInstance() ).thenReturn( statementLocks );

        JobScheduler jobScheduler = mock( JobScheduler.class, RETURNS_MOCKS );
        Monitors monitors = new Monitors();
        dataSource = new NeoStoreDataSource( storeDir, config, idGeneratorFactory, IdReuseEligibility.ALWAYS,
                idConfigurationProvider,
                logService, mock( JobScheduler.class, RETURNS_MOCKS ), mock( TokenNameLookup.class ),
                dependencyResolverForNoIndexProvider(), mock( PropertyKeyTokenHolder.class ),
                mock( LabelTokenHolder.class ), mock( RelationshipTypeTokenHolder.class ), locksFactory,
                mock( SchemaWriteGuard.class ), mock( TransactionEventHandlers.class ), IndexingService.NO_MONITOR,
                fs, mock( TransactionMonitor.class ), databaseHealth,
                mock( PhysicalLogFile.Monitor.class ), TransactionHeaderInformationFactory.DEFAULT,
                new StartupStatisticsProvider(), null,
                new CommunityCommitProcessFactory(), mock( InternalAutoIndexing.class ), pageCache,
                new StandardConstraintSemantics(), monitors,
                new Tracers( "null", NullLog.getInstance(), monitors, jobScheduler ),
                mock( Procedures.class ),
                IOLimiter.unlimited() );

        return dataSource;
    }

    public NeoStoreDataSource getDataSource( File storeDir, FileSystemAbstraction fs,
            PageCache pageCache, Map<String,String> additionalConfig )
    {
        DatabaseHealth databaseHealth = new DatabaseHealth( mock( DatabasePanicEventGenerator.class ),
                NullLogProvider.getInstance().getLog( DatabaseHealth.class ) );
        return getDataSource( storeDir, fs, pageCache, additionalConfig, databaseHealth );
    }

    private DependencyResolver dependencyResolverForNoIndexProvider()
    {
        return new DependencyResolver.Adapter()
        {
            private final LabelScanStoreProvider labelScanStoreProvider =
                    new LabelScanStoreProvider( new InMemoryLabelScanStore(), 10 );

            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
            {
                if ( SchemaIndexProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( SchemaIndexProvider.NO_INDEX_PROVIDER );
                }
                else if ( LabelScanStoreProvider.class.isAssignableFrom( type ) )
                {
                    return type.cast( labelScanStoreProvider );
                }
                throw new IllegalArgumentException( type.toString() );
            }
        };
    }
}
