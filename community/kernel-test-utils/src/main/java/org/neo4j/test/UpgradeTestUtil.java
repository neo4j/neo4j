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
package org.neo4j.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class UpgradeTestUtil {
    public static void upgradeDatabase(
            DatabaseManagementService dbms,
            GraphDatabaseAPI db,
            KernelVersion expectedCurrentVersion,
            KernelVersion expectedUpgradedVersions) {
        assertKernelVersion(db, expectedCurrentVersion);

        upgradeDbms(dbms);
        createWriteTransaction(db);

        assertKernelVersion(db, expectedUpgradedVersions);
    }

    public static void createWriteTransaction(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
    }

    private static KernelVersion getKernelVersion(GraphDatabaseAPI db) {
        return db.getDependencyResolver()
                .resolveDependency(KernelVersionProvider.class)
                .kernelVersion();
    }

    public static void assertKernelVersion(GraphDatabaseAPI database, KernelVersion expectedVersion) {
        assertThat(getKernelVersion(database)).isEqualTo(expectedVersion);
    }

    public static void upgradeDbms(DatabaseManagementService dbms) {
        GraphDatabaseAPI system = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        try (var tx = system.beginTx()) {
            tx.execute("CALL dbms.upgrade()").close();
            tx.commit();
        }
    }
}
