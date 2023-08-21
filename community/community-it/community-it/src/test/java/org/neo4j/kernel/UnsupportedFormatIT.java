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
package org.neo4j.kernel;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class UnsupportedFormatIT {

    @Inject
    private TestDirectory testDirectory;

    @ParameterizedTest
    @ValueSource(strings = {"block", "high_limit"})
    void startDbmsOnEnterpriseFormatInCommunityShouldNotFailDbmsStartup(String enterpriseFormat) {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(GraphDatabaseSettings.db_format, enterpriseFormat)
                .build();
        try {
            // System db should have started up and be on aligned format
            GraphDatabaseAPI system =
                    (GraphDatabaseAPI) managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
            assertThat(system.isAvailable()).isTrue();
            MetadataProvider metadataProvider =
                    system.getDependencyResolver().resolveDependency(MetadataProvider.class);
            assertThat(metadataProvider.getStoreId().getFormatName()).isEqualTo(PageAligned.LATEST_NAME);
        } finally {
            managementService.shutdown();
        }
    }
}
