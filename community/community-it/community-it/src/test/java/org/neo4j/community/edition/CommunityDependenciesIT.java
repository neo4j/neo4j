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
package org.neo4j.community.edition;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class CommunityDependenciesIT {
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;

    @BeforeEach
    void setUp() {
        dbms = new TestDatabaseManagementServiceBuilder(testDirectory.homePath()).build();
    }

    @AfterEach
    void tearDown() {
        dbms.shutdown();
    }

    @Test
    void ensureHasTheCorrectDependencies() {
        var dependencyResolver = ((GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();

        assertThat(dependencyResolver.resolveDependency(TopologyGraphDbmsModel.HostedOnMode.class))
                .isEqualTo(TopologyGraphDbmsModel.HostedOnMode.SINGLE);
    }
}
