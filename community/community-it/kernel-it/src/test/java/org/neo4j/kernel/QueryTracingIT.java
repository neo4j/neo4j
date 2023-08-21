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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class QueryTracingIT {
    @Inject
    private GraphDatabaseAPI databaseAPI;

    private static final List<ExecutingQuery> queries = new ArrayList<>();

    @ExtensionCallback
    protected void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.addExtension(new CustomProcedureExtensionFactory());
    }

    @Test
    void chainedQueryExecution() {
        try (var transaction = databaseAPI.beginTransaction(IMPLICIT, AUTH_DISABLED);
                var statement =
                        (KernelStatement) transaction.kernelTransaction().acquireStatement()) {
            QueryRegistry queryRegistry = statement.queryRegistry();
            assertTrue(queryRegistry.executingQuery().isEmpty());

            assertThat(queries).isEmpty();
            var outerQuery = "CALL db.testCaptureProcedure()";
            var innerQuery = "match (n) return count(n)";
            try (Result result = transaction.execute(outerQuery)) {
                result.next();
            }

            assertThat(queries).hasSize(3);
            assertThat(queries.get(0).rawQueryText()).isEqualTo(outerQuery);
            assertThat(queries.get(1).rawQueryText()).isEqualTo(innerQuery);
            assertThat(queries.get(2).rawQueryText()).isEqualTo(outerQuery);
        }
    }

    public static class QueryCapturingProcedure {
        @Context
        public Transaction transaction;

        @Procedure(name = "db.testCaptureProcedure")
        public Stream<Result> myProc() {
            var statement = (KernelStatement)
                    ((InternalTransaction) transaction).kernelTransaction().acquireStatement();
            queries.add(statement.queryRegistry().executingQuery().get());
            try (var result = transaction.execute("match (n) return count(n)")) {
                queries.add(statement.queryRegistry().executingQuery().get());
            }
            queries.add(statement.queryRegistry().executingQuery().get());
            return Stream.of(new Result());
        }

        public static class Result {
            public Long value = 7L;
        }
    }

    private static class CustomProcedureExtensionFactory
            extends ExtensionFactory<CustomProcedureExtensionFactory.Dependencies> {
        protected CustomProcedureExtensionFactory() {
            super("customProcedureFactory");
        }

        interface Dependencies {
            GlobalProcedures procedures();
        }

        @Override
        public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
            try {
                dependencies.procedures().registerProcedure(QueryCapturingProcedure.class);
            } catch (KernelException e) {
                throw new RuntimeException(e);
            }
            return new LifecycleAdapter();
        }
    }
}
