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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class DefaultIndexesIT {

    @Inject
    private GraphDatabaseService db;

    @Inject
    DbmsController dbmsController;

    @Test
    void defaultIndexesCreatedOnFirstStart() {
        IndexingTestUtil.assertOnlyDefaultTokenIndexesExists(db);
    }

    @Test
    void defaultIndexesAreNotRecreatedAfterDropAndRestart() {
        try (var tx = db.beginTx()) {
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }

        dbmsController.restartDbms();

        assertThat(db.isAvailable(TimeUnit.MINUTES.toMillis(5))).isTrue();

        try (var tx = db.beginTx()) {
            assertThat(tx.schema().getIndexes()).isEmpty();
        }
    }
}
