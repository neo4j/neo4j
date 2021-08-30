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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

/**
 * IndexProviderFactory that can be loaded as extension and delegates to built in index provider.
 * It needs to override descriptor to avoid clash with builtin descriptors.
 */
public class BuiltInDelegatingIndexProviderFactory extends ExtensionFactory<BuiltInDelegatingIndexProviderFactory.Dependencies>
{

    private final AbstractIndexProviderFactory<?> delegate;
    private final IndexProviderDescriptor descriptorOverride;

    public BuiltInDelegatingIndexProviderFactory( AbstractIndexProviderFactory<?> delegate, IndexProviderDescriptor descriptorOverride )
    {
        super( ExtensionType.DATABASE, descriptorOverride.getKey() );
        this.delegate = delegate;
        this.descriptorOverride = descriptorOverride;
    }

    @Override
    public IndexProvider newInstance( ExtensionContext context, Dependencies dependencies )
    {
        var provider = delegate.create(
                dependencies.pageCache(), dependencies.fileSystem(), dependencies.getLogService(), dependencies.monitors(), dependencies.getConfig(),
                dependencies.readOnlyChecker(), context.dbmsInfo(), dependencies.recoveryCleanupWorkCollector(),
                dependencies.databaseTracer().getPageCacheTracer(), dependencies.databaseLayout(), dependencies.tokenHolders(), dependencies.jobScheduler() );
        return new IndexProvider.Delegating( provider )
        {
            @Override
            public IndexProviderDescriptor getProviderDescriptor()
            {
                return descriptorOverride;
            }

            @Override
            public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache,
                                                                        StorageEngineFactory storageEngineFactory )
            {
                return new NameOverridingStoreMigrationParticipant( super.storeMigrationParticipant( fs, pageCache, storageEngineFactory ),
                                                                    descriptorOverride.name() );
            }
        };
    }

    public interface Dependencies
    {
        PageCache pageCache();

        FileSystemAbstraction fileSystem();

        LogService getLogService();

        Monitors monitors();

        Config getConfig();

        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector();

        DatabaseLayout databaseLayout();

        DatabaseTracers databaseTracer();

        DatabaseReadOnlyChecker readOnlyChecker();

        TokenHolders tokenHolders();

        JobScheduler jobScheduler();
    }
}
