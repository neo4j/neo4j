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
package org.neo4j.bolt.fsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.bolt.fsm.AutocommitIT.IRIS_DATA;
import static org.neo4j.bolt.fsm.AutocommitIT.createLocalIrisData;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.values.storable.Values.longValue;

import org.neo4j.bolt.test.annotation.CommunityStateMachineTestExtension;
import org.neo4j.bolt.testing.annotation.fsm.StateMachineTest;
import org.neo4j.bolt.testing.annotation.fsm.initializer.Authenticated;
import org.neo4j.bolt.testing.extension.provider.TransactionIdProvider;
import org.neo4j.bolt.testing.messages.BoltMessages;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.ValueUtils;

@CommunityStateMachineTestExtension
class CallInTransactionsAutocommitIT {

    @StateMachineTest
    void shouldSupportUsingCallInTransactionsInSession(
            @Authenticated StateMachine fsm,
            BoltMessages messages,
            ResponseRecorder recorder,
            TransactionIdProvider idProvider)
            throws Exception {
        var params = ValueUtils.asMapValue(MapUtil.map("csvFileUrl", createLocalIrisData(fsm, messages)));
        var txIdBeforeQuery = idProvider.latest();
        var batch = 40;

        fsm.process(
                messages.run(
                        joinAsLines(
                                "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l",
                                "CALL {",
                                "  WITH l",
                                "  MATCH (c:Class {name: l.class_name})",
                                "  CREATE (s:Sample {sepal_length: l.sepal_length,",
                                "                    sepal_width: l.sepal_width,",
                                "                    petal_length: l.petal_length,",
                                "                    petal_width: l.petal_width})",
                                "  CREATE (c)<-[:HAS_CLASS]-(s)",
                                "  RETURN c, s",
                                "} IN TRANSACTIONS OF 40 ROWS",
                                "RETURN count(*) AS c"),
                        params),
                recorder);
        fsm.process(messages.pull(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse().hasRecord(longValue(150L)).hasSuccessResponse();

        /*
         * 7 tokens have been created for
         * 'Sample' label
         * 'HAS_CLASS' relationship type
         * 'name', 'sepal_length', 'sepal_width', 'petal_length', and 'petal_width' property keys
         *
         * Note that the token id for the label 'Class' has been created in `createLocalIrisData(...)` so it shouldn't1
         * be counted again here
         */
        var tokensCommits = 7;
        var commits = (IRIS_DATA.split("\n").length - 1 /* header */) / batch;
        var txId = idProvider.latest();
        assertEquals(tokensCommits + commits + txIdBeforeQuery, txId);
    }
}
