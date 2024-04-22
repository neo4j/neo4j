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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class LogFileHeaderAppendIndexIT {
    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Inject
    private LogFiles logFiles;

    @Inject
    private MetadataProvider metadataProvider;

    @Test
    void logFileHeaderContainsLastIndex() throws IOException {
        LogFile logFile = logFiles.getLogFile();
        assertEquals(
                BASE_APPEND_INDEX,
                logFile.extractHeader(logFile.getCurrentLogVersion()).getLastAppendIndex());

        RelationshipType relationshipType = RelationshipType.withName("marker");
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < 42; i++) {
                try (Transaction transaction = databaseAPI.beginTx()) {
                    Node start = transaction.createNode();
                    Node end = transaction.createNode();
                    start.createRelationshipTo(end, relationshipType);
                    transaction.commit();
                }
            }

            logFile.rotate();

            long currentHeaderLastAppendIndex =
                    logFile.extractHeader(logFile.getCurrentLogVersion()).getLastAppendIndex();
            assertThat(currentHeaderLastAppendIndex)
                    .isGreaterThan(BASE_APPEND_INDEX)
                    .isEqualTo(metadataProvider.getLastAppendIndex());
        }
    }
}
