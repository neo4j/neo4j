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
package org.neo4j.kernel.api.impl.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.LATEST_INDEX_PROVIDERS;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.IncorrectFormat;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class FailingIndexProviderIT {
    private static final String DUMP = "fulltext-with-missing-custom-analyzer.dump";

    @Inject
    TestDirectory directory;

    @Inject
    FileSystemAbstraction fs;

    private DatabaseLayout databaseLayout;
    private Config config;

    @BeforeEach
    void setUp() {
        config = Config.defaults(GraphDatabaseSettings.neo4j_home, directory.homePath());
        databaseLayout = DatabaseLayout.of(config);
    }

    @Test
    public void indexShouldBeInFailedStateWhenIndexDataIsDeleted()
            throws IOException, URISyntaxException, IncorrectFormat {
        // Given an index with custom analyzer that is not presented in the classpath
        String databaseName = Config.defaults().get(GraphDatabaseSettings.initial_default_database);
        Path dump =
                Path.of(getClass().getResource(DUMP).toURI()).toAbsolutePath().normalize();
        fs.mkdirs(databaseLayout.databaseDirectory());
        fs.mkdirs(databaseLayout.getNeo4jLayout().transactionLogsRootDirectory());
        Loader loader = new Loader(fs);
        loader.load(databaseLayout, dump);
        // the name of the index was set when dump was created
        String indexName = "index1";
        int indexId = 3;
        Path indexDirectory = indexDirectoryFor(databaseLayout.databaseDirectory(), IndexType.FULLTEXT, indexId);

        // when index directory is removed
        fs.deleteRecursively(indexDirectory);

        // then start of database doesn't fail and index is in failed state
        try (DatabaseManagementService dbms = startDbms()) {
            GraphDatabaseService database = dbms.database(databaseName);
            IndexProxy failedIndex = getIndexProxyByName(database, indexName);
            assertThat(failedIndex.getState()).isEqualTo(InternalIndexState.FAILED);
            assertThat(failedIndex.getPopulationFailure().asString()).isNotEmpty();
        }
    }

    private static IndexProxy getIndexProxyByName(GraphDatabaseService database, String indexName) {
        Iterable<IndexProxy> proxies = ((GraphDatabaseAPI) database)
                .getDependencyResolver()
                .resolveDependency(IndexingService.class)
                .getIndexProxies();
        for (IndexProxy proxy : proxies) {
            if (proxy.getDescriptor().getName().equals(indexName)) {
                return proxy;
            }
        }
        throw new IllegalStateException("Wasn't able to find index proxy with name " + indexName);
    }

    private static Path indexDirectoryFor(Path dbPath, IndexType indexType, long indexId) {
        return IndexDirectoryStructure.directoriesByProvider(dbPath)
                .forProvider(LATEST_INDEX_PROVIDERS.get(indexType))
                .directoryForIndex(indexId);
    }

    private DatabaseManagementService startDbms() {
        return new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setConfig(config)
                .build();
    }
}
