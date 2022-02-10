/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.PointIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.RangeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.TextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.TokenIndexProviderFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

public class StaticIndexProviderMapFactory
{

    public static StaticIndexProviderMap create( LifeSupport life, Config databaseConfig, PageCache pageCache,
                                                 FileSystemAbstraction fs, LogService logService, Monitors monitors,
                                                 DatabaseReadOnlyChecker readOnlyChecker, DbmsInfo dbmsInfo,
                                                 RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                                                 DatabaseLayout databaseLayout, TokenHolders tokenHolders, JobScheduler scheduler,
                                                 CursorContextFactory contextFactory )
    {
        return create( life, databaseConfig, pageCache, fs, logService, monitors, readOnlyChecker, dbmsInfo, recoveryCleanupWorkCollector,
                       databaseLayout, tokenHolders, scheduler, contextFactory, new Dependencies() );
    }

    public static StaticIndexProviderMap create(
            LifeSupport life, Config databaseConfig, PageCache pageCache,
            FileSystemAbstraction fs, LogService logService, Monitors monitors,
            DatabaseReadOnlyChecker readOnlyChecker, DbmsInfo dbmsInfo,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout, TokenHolders tokenHolders, JobScheduler scheduler, CursorContextFactory contextFactory,
            DependencyResolver dependencies )
    {
        var tokenIndexProvider = life.add( new TokenIndexProviderFactory().create(
                pageCache, fs, logService, monitors, databaseConfig, readOnlyChecker, dbmsInfo,
                recoveryCleanupWorkCollector, databaseLayout, tokenHolders, scheduler, contextFactory ) );

        var nativeIndexProvider = life.add( new GenericNativeIndexProviderFactory().create(
                pageCache, fs, logService, monitors, databaseConfig, readOnlyChecker, dbmsInfo,
                recoveryCleanupWorkCollector, databaseLayout, tokenHolders, scheduler, contextFactory ) );

        var textIndexProvider = life.add( new TextIndexProviderFactory().create(
                pageCache, fs, logService, monitors, databaseConfig, readOnlyChecker, dbmsInfo,
                recoveryCleanupWorkCollector, databaseLayout, tokenHolders, scheduler, contextFactory ) );

        var fulltextIndexProvider = life.add( new FulltextIndexProviderFactory().create(
                pageCache, fs, logService, monitors, databaseConfig, readOnlyChecker, dbmsInfo,
                recoveryCleanupWorkCollector, databaseLayout, tokenHolders, scheduler, contextFactory ) );

        var rangeIndexProvider = life.add( new RangeIndexProviderFactory().create(
                pageCache, fs, logService, monitors, databaseConfig, readOnlyChecker, dbmsInfo,
                recoveryCleanupWorkCollector, databaseLayout, tokenHolders, scheduler, contextFactory ) );

        var pointIndexProvider = life.add( new PointIndexProviderFactory().create(
                pageCache, fs, logService, monitors, databaseConfig, readOnlyChecker, dbmsInfo,
                recoveryCleanupWorkCollector, databaseLayout, tokenHolders, scheduler, contextFactory ) );

        return new StaticIndexProviderMap( tokenIndexProvider, nativeIndexProvider, textIndexProvider, fulltextIndexProvider,
                                           rangeIndexProvider, pointIndexProvider, databaseConfig, dependencies );
    }
}
