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
package org.neo4j.procedure.impl;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

class ProcedureGraphDatabaseAPITest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("beginTxCalls")
    void allTransactionsGetLoginContextTransformed(String name, Consumer<GraphDatabaseAPI> call) {
        var graphDatabaseAPI = mock(GraphDatabaseAPI.class);
        var loginContextCaptor = ArgumentCaptor.forClass(LoginContext.class);
        var internalTransaction = mock(InternalTransaction.class);
        when(graphDatabaseAPI.beginTransaction(any(), loginContextCaptor.capture(), any(), anyLong(), any()))
                .thenReturn(internalTransaction);

        var transformedLoginContext = mock(LoginContext.class);
        var procedureGraphDatabaseService =
                new ProcedureGraphDatabaseAPI(graphDatabaseAPI, ctx -> transformedLoginContext, Config.defaults());

        call.accept(procedureGraphDatabaseService);

        assertThat(loginContextCaptor.getValue()).isSameAs(transformedLoginContext);
    }

    @SuppressWarnings("Convert2MethodRef")
    private static List<Arguments> beginTxCalls() {
        return List.of(
                Arguments.of("beginTx() 1", (Consumer<GraphDatabaseAPI>) api -> api.beginTx()),
                Arguments.of("beginTx() 2", (Consumer<GraphDatabaseAPI>) api -> api.beginTx(1, MINUTES)),
                Arguments.of("beginTransaction() 1", (Consumer<GraphDatabaseAPI>)
                        api -> api.beginTransaction(EXPLICIT, AUTH_DISABLED)),
                Arguments.of("beginTransaction() 2", (Consumer<GraphDatabaseAPI>)
                        api -> api.beginTransaction(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION)),
                Arguments.of("beginTransaction() 3", (Consumer<GraphDatabaseAPI>)
                        api -> api.beginTransaction(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, 1, MINUTES)));
    }
}
