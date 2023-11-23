/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.index;

import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import org.mockito.Mockito;
import org.neo4j.common.EmptyDependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.dbms.database.readonly.DefaultReadOnlyDatabases;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.index.schema.TokenIndexProviderFactory;
import org.neo4j.monitoring.Monitors;

class TokenIndexProviderCompatibilitySuiteTest extends SpecialisedIndexProviderCompatibilityTestSuite {
    @Override
    IndexPrototype indexPrototype() {
        return IndexPrototype.forSchema(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR);
    }

    @Override
    IndexType indexType() {
        return IndexType.LOOKUP;
    }

    @Override
    IndexProvider createIndexProvider(PageCache pageCache, FileSystemAbstraction fs, Path graphDbDir, Config config) {
        Monitors monitors = new Monitors();
        String monitorTag = "";
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
        DatabaseLayout databaseLayout = DatabaseLayout.ofFlat(graphDbDir);
        var defaultDatabaseId = DatabaseIdFactory.from(
                DEFAULT_DATABASE_NAME, UUID.randomUUID()); // UUID required, but ignored by config lookup
        DatabaseIdRepository databaseIdRepository = mock(DatabaseIdRepository.class);
        Mockito.when(databaseIdRepository.getByName(DEFAULT_DATABASE_NAME)).thenReturn(Optional.of(defaultDatabaseId));
        var readOnlyDatabases =
                new DefaultReadOnlyDatabases(new ConfigBasedLookupFactory(config, databaseIdRepository));
        var readOnlyChecker = readOnlyDatabases.forDatabase(defaultDatabaseId);
        return TokenIndexProviderFactory.create(
                pageCache,
                graphDbDir,
                fs,
                monitors,
                monitorTag,
                readOnlyChecker,
                recoveryCleanupWorkCollector,
                databaseLayout,
                NULL_CONTEXT_FACTORY,
                NULL,
                EmptyDependencyResolver.EMPTY_RESOLVER);
    }
}
