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
package org.neo4j.bolt.testing.extension.initializer.mock;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.mockito.internal.util.MockUtil;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.testing.annotation.fsm.initializer.mock.MockAutocommit;
import org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions;
import org.neo4j.bolt.testing.assertions.StateMachineAssertions;
import org.neo4j.bolt.testing.extension.dependency.StateMachineDependencyProvider;
import org.neo4j.bolt.testing.extension.initializer.StateMachineInitializer;
import org.neo4j.bolt.testing.fsm.StateMachineProvider;
import org.neo4j.bolt.testing.mock.MockResult;
import org.neo4j.bolt.testing.mock.StatementMockFactory;
import org.neo4j.bolt.testing.mock.TransactionManagerMockFactory;
import org.neo4j.bolt.testing.mock.TransactionMockFactory;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.values.storable.Values;

public class MockAutocommitStateMachineInitializer implements StateMachineInitializer {

    @Override
    public void initialize(
            ExtensionContext extensionContext,
            ParameterContext parameterContext,
            StateMachineDependencyProvider dependencyProvider,
            StateMachineProvider provider,
            StateMachine fsm)
            throws StateMachineException {
        var recorder = new ResponseRecorder();

        var n = AnnotationUtils.findAnnotation(parameterContext.getParameter(), MockAutocommit.class)
                .map(annotation -> annotation.results())
                .orElse(1);

        var transactionManager = dependencyProvider
                .transactionManager()
                .filter(MockUtil::isMock)
                .orElseThrow(
                        () -> new IllegalStateException("Cannot apply mock initialization within this environment"));

        TransactionManagerMockFactory.newFactory()
                .withFactory((type, owner, databaseName, mode, bookmarks, timeout, metadata) ->
                        TransactionMockFactory.newFactory()
                                .withFactory((statement, params) -> StatementMockFactory.newFactory()
                                        .withResults(MockResult.newFactory()
                                                .withField("n")
                                                .withSimpleRecords(n, i -> Values.longValue(i))
                                                .build())
                                        .build())
                                .build())
                .apply(transactionManager);

        fsm.process(provider.messages().run("UNWIND RANGE(0, " + n + ") AS n RETURN n"), recorder, null);

        ResponseRecorderAssertions.assertThat(recorder).hasSuccessResponse();
        StateMachineAssertions.assertThat(fsm).isInState(States.AUTO_COMMIT);
    }
}
