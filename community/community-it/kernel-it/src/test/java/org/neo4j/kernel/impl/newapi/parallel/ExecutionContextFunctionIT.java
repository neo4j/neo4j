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
package org.neo4j.kernel.impl.newapi.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.FULL;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.PathWrappingPathValue;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.NodeIdReference;
import org.neo4j.values.virtual.VirtualValues;

@DbmsExtension(configurationCallback = "configuration")
class ExecutionContextFunctionIT {

    private static final String RUNTIME_USED = "TEST";

    @Inject
    private GraphDatabaseAPI db;

    @BeforeEach
    void beforeEach() throws KernelException {
        registerFunctions();
    }

    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(
                GraphDatabaseSettings.procedure_unrestricted,
                List.of("execution.context.test.function.doSomethingWithKernelTransaction"));
    }

    @Test
    void testUserFunctionAcceptingBasicType() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "plus", Values.intValue(1), Values.intValue(2));
            assertThat(result).isEqualTo(Values.intValue(3));
        });
    }

    @Test
    void testUserFunctionAcceptingNode() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "nodeId", VirtualValues.node(123));
            assertThat(result).isEqualTo(Values.intValue(123));
        });
    }

    @Test
    void testUserFunctionAcceptingNodeList() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(
                    executionContext, "nodeIds", VirtualValues.list(VirtualValues.node(123), VirtualValues.node(456)));
            assertThat(result).isEqualTo(VirtualValues.list(Values.intValue(123), Values.intValue(456)));
        });
    }

    @Test
    void testUserFunctionAcceptingRelationship() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "relationshipId", VirtualValues.relationship(123));
            assertThat(result).isEqualTo(Values.longValue(123));
        });
    }

    @Test
    void testUserFunctionAcceptingRelationshipList() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(
                    executionContext,
                    "relationshipIds",
                    VirtualValues.list(VirtualValues.relationship(123), VirtualValues.relationship(456)));
            assertThat(result).isEqualTo(VirtualValues.list(Values.intValue(123), Values.intValue(456)));
        });
    }

    @Test
    void userFunctionShouldNotWrapExecutionContextNodes() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "passThrough", VirtualValues.node(123));
            assertThat(result).isNotInstanceOf(NodeEntityWrappingNodeValue.class);
        });
    }

    @Test
    void userFunctionShouldNotWrapExecutionContextRelationships() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "passThrough", VirtualValues.relationship(123));
            assertThat(result).isNotInstanceOf(RelationshipEntityWrappingValue.class);
        });
    }

    @Test
    void userFunctionShouldNotWrapExecutionContextPaths() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var path = VirtualValues.pathReference(new long[] {11, 22}, new long[] {33});
            AnyValue result = invokeUserFunction(executionContext, "passThrough", path);
            assertThat(result).isNotInstanceOf(PathWrappingPathValue.class);
        });
    }

    @Test
    void userFunctionShouldNotHandleWrappedNodesAsReferences() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var originalWrappedNode = mock(Node.class);
            AnyValue result =
                    invokeUserFunction(executionContext, "passThrough", ValueUtils.wrapNodeEntity(originalWrappedNode));
            assertThat(result).isInstanceOf(NodeEntityWrappingNodeValue.class);
            var unwrappedNode = ((NodeEntityWrappingNodeValue) result).getEntity();
            assertThat(unwrappedNode).isEqualTo(originalWrappedNode);
        });
    }

    @Test
    void userFunctionShouldNotHandleWrappedRelationshipsAsReferences() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var originalWrappedRelationship = mock(Relationship.class);
            AnyValue result = invokeUserFunction(
                    executionContext, "passThrough", ValueUtils.wrapRelationshipEntity(originalWrappedRelationship));
            assertThat(result).isInstanceOf(RelationshipEntityWrappingValue.class);
            var unwrappedRelationship = ((RelationshipEntityWrappingValue) result).getEntity();
            assertThat(unwrappedRelationship).isEqualTo(originalWrappedRelationship);
        });
    }

    @Test
    void userFunctionShouldNotHandleWrappedPathsAsReferences() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var originalWrappedPath = mock(Path.class);
            AnyValue result =
                    invokeUserFunction(executionContext, "passThrough", ValueUtils.wrapPath(originalWrappedPath));
            assertThat(result).isInstanceOf(PathWrappingPathValue.class);
            var unwrappedPath = ((PathWrappingPathValue) result).path();
            assertThat(unwrappedPath).isSameAs(originalWrappedPath);
        });
    }

    @Test
    void testUserFunctionUsingUnsupportedNodeOperation() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            assertThatThrownBy(() -> invokeUserFunction(executionContext, "deleteNode", VirtualValues.node(123)))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Operation unsupported during parallel query execution");
        });
    }

    @Test
    void testUserFunctionUsingUnsupportedRelationshipOperation() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            assertThatThrownBy(() ->
                            invokeUserFunction(executionContext, "deleteRelationship", VirtualValues.relationship(123)))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Operation unsupported during parallel query execution");
        });
    }

    @Test
    void testInjectingTransactionIntoUserFunctionAndGettingDataFromIt() throws ProcedureException {
        NodeIdReference nodeRef;
        String nodeId;
        try (Transaction tx = db.beginTx()) {
            var node = tx.createNode();
            nodeId = node.getElementId();
            nodeRef = VirtualValues.node(node.getId());
            tx.commit();
        }

        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "getNodeById", Values.of(nodeId));
            assertThat(result).isEqualTo(nodeRef);
        });
    }

    @Test
    void testKernelTransactionInjectionIntoUserFunction() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            assertThatThrownBy(() -> invokeUserFunction(
                            executionContext, "doSomethingWithKernelTransaction", Values.intValue(1)))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "`transaction.setMetaData' is not supported in procedures when called from parallel runtime. Please retry using another runtime.");
        });
    }

    @Test
    void testGraphDatabaseServiceInjectionIntoUserFunction() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeUserFunction(executionContext, "databaseName");
            assertThat(result).isEqualTo(Values.stringValue(db.databaseName()));
        });
    }

    @Test
    void testUserFunctionSecurityContext() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            // We should start with FULL access mode ...
            AccessMode originalAccessMode = executionContext.securityContext().mode();
            assertThat(originalAccessMode).isEqualTo(FULL);

            // ... which should be restricted to READ during the function call ...
            AnyValue result = invokeUserFunction(executionContext, "accessMode");
            assertThat(result)
                    .isEqualTo(Values.stringValue(
                            new OverriddenAccessMode(originalAccessMode, AccessMode.Static.READ).name()));

            // ... and restored to FULL again after the call.
            assertThat(executionContext.securityContext().mode()).isEqualTo(FULL);
        });
    }

    @Test
    void closedTransactionShouldBeDetectedOnUserFunctionInvocation() {
        try (Transaction transaction = db.beginTx();
                Statement statement = acquireStatement(transaction);
                ExecutionContext executionContext = createExecutionContext(transaction)) {
            try {
                var handle = executionContext.procedures().functionGet(getName("plus"), CypherScope.CYPHER_5);
                var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

                transaction.rollback();
                assertThatThrownBy(() -> executionContext
                                .procedures()
                                .functionCall(
                                        handle.id(), new Value[] {Values.intValue(1), Values.intValue(2)}, procContext))
                        .isInstanceOf(NotInTransactionException.class)
                        .hasMessageContaining("This transaction has already been closed.");
            } finally {
                executionContext.complete();
            }
        }
    }

    @Test
    void testUserAggregationFunctionAcceptingBasicType() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var sumFunction = prepareUserAggregationFunction(executionContext, "sum");
            var updater = sumFunction.newUpdater();
            updater.update(new Value[] {Values.intValue(1)});
            updater.update(new Value[] {Values.intValue(2)});
            updater.update(new Value[] {Values.intValue(3)});
            updater.applyUpdates();

            assertThat(sumFunction.result()).isEqualTo(Values.intValue(6));
        });
    }

    @Test
    void testUserAggregationFunctionAcceptingNode() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var sumFunction = prepareUserAggregationFunction(executionContext, "sumNodeIds");
            var updater = sumFunction.newUpdater();
            updater.update(new AnyValue[] {VirtualValues.node(1)});
            updater.update(new AnyValue[] {VirtualValues.node(2)});
            updater.update(new AnyValue[] {VirtualValues.node(3)});
            updater.applyUpdates();

            assertThat(sumFunction.result()).isEqualTo(Values.intValue(6));
        });
    }

    @Test
    void closedTransactionShouldBeDetectedOnUserAggregationFunctionInvocation() {
        try (Transaction transaction = db.beginTx();
                Statement statement = acquireStatement(transaction);
                ExecutionContext executionContext = createExecutionContext(transaction)) {
            try {
                var handle = executionContext.procedures().aggregationFunctionGet(getName("sum"), CypherScope.CYPHER_5);
                transaction.rollback();
                assertThatThrownBy(() -> executionContext
                                .procedures()
                                .aggregationFunction(handle.id(), ProcedureCallContext.EMPTY))
                        .isInstanceOf(NotInTransactionException.class)
                        .hasMessageContaining("This transaction has already been closed.");
            } finally {
                executionContext.complete();
            }
        }
    }

    @Test
    void testBuiltInFunction() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var handle = executionContext.procedures().functionGet(new QualifiedName("date"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

            AnyValue result = executionContext
                    .procedures()
                    .builtInFunctionCall(handle.id(), new AnyValue[] {Values.stringValue("2022-10-07")}, procContext);
            assertThat(result).isEqualTo(DateValue.date(LocalDate.parse("2022-10-07")));
        });
    }

    @Test
    void testRealClockTemporalFunction() throws ProcedureException {
        doWithExecutionContext((ktx, executionContext) -> {
            ZonedDateTime referenceDateTime = ZonedDateTime.now();
            var handle = executionContext
                    .procedures()
                    .functionGet(new QualifiedName("datetime", "realtime"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

            AnyValue result =
                    executionContext.procedures().builtInFunctionCall(handle.id(), new AnyValue[0], procContext);
            assertThat(result).isInstanceOf(DateTimeValue.class);
            // Why equal? The clock is not ticking quick enough on some platforms to return different time
            // for two requests for current time following quickly after each other
            assertThat(((DateTimeValue) result).asObjectCopy()).isAfterOrEqualTo(referenceDateTime);
        });
    }

    @Test
    void testTransactionClockTemporalFunction() throws ProcedureException {
        doWithExecutionContext((ktx, executionContext) -> {
            var handle = executionContext
                    .procedures()
                    .functionGet(new QualifiedName("datetime", "transaction"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

            AnyValue result =
                    executionContext.procedures().builtInFunctionCall(handle.id(), new AnyValue[0], procContext);
            assertThat(result)
                    .isEqualTo(DateTimeValue.datetime(
                            ZonedDateTime.now(ktx.clocks().transactionClock())));
        });
    }

    @Test
    void testStatementClockTemporalFunction() throws ProcedureException {
        doWithExecutionContext((ktx, executionContext) -> {
            var handle = executionContext
                    .procedures()
                    .functionGet(new QualifiedName("datetime", "statement"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

            AnyValue result =
                    executionContext.procedures().builtInFunctionCall(handle.id(), new AnyValue[0], procContext);
            assertThat(result)
                    .isEqualTo(DateTimeValue.datetime(
                            ZonedDateTime.now(ktx.clocks().statementClock())));
        });
    }

    @Test
    void testDefaultClockTemporalFunction() throws ProcedureException {
        doWithExecutionContext((ktx, executionContext) -> {
            var handle = executionContext.procedures().functionGet(new QualifiedName("datetime"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

            AnyValue result =
                    executionContext.procedures().builtInFunctionCall(handle.id(), new AnyValue[0], procContext);
            assertThat(result)
                    .isEqualTo(DateTimeValue.datetime(
                            ZonedDateTime.now(ktx.clocks().statementClock())));
        });
    }

    void doWithExecutionContext(ExecutionContextLogic executionContextLogic) throws ProcedureException {
        try (Transaction transaction = db.beginTx();
                Statement statement = acquireStatement(transaction);
                ExecutionContext executionContext = createExecutionContext(transaction)) {
            try {
                executionContextLogic.doWithExecutionContext(executionContext);
            } finally {
                executionContext.complete();
            }
        }
    }

    void doWithExecutionContext(ExecutionContextLogic2 executionContextLogic) throws ProcedureException {
        try (Transaction transaction = db.beginTx();
                Statement statement = acquireStatement(transaction);
                ExecutionContext executionContext = createExecutionContext(transaction)) {
            try {
                executionContextLogic.doWithExecutionContext(
                        ((KernelTransactionImplementation) ((InternalTransaction) transaction).kernelTransaction()),
                        executionContext);
            } finally {
                executionContext.complete();
            }
        }
    }

    private Statement acquireStatement(Transaction transaction) {
        return ((InternalTransaction) transaction).kernelTransaction().acquireStatement();
    }

    private AnyValue invokeUserFunction(ExecutionContext executionContext, String name, AnyValue... args)
            throws ProcedureException {
        var handle = executionContext.procedures().functionGet(getName(name), CypherScope.CYPHER_5);
        var procContext = new ProcedureCallContext(handle.id(), new String[0], false, "", false, RUNTIME_USED);

        return executionContext.procedures().functionCall(handle.id(), args, procContext);
    }

    private UserAggregationReducer prepareUserAggregationFunction(ExecutionContext executionContext, String name)
            throws ProcedureException {
        var handle = executionContext.procedures().aggregationFunctionGet(getName(name), CypherScope.CYPHER_5);
        return executionContext.procedures().aggregationFunction(handle.id(), ProcedureCallContext.EMPTY);
    }

    private QualifiedName getName(String name) {
        return new QualifiedName("execution", "context", "test", "function", name);
    }

    private ExecutionContext createExecutionContext(Transaction transaction) {
        return ((InternalTransaction) transaction).kernelTransaction().createExecutionContext();
    }

    private void registerFunctions() throws KernelException {
        var globalProcedures = db.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        globalProcedures.registerFunction(BasicTestFunctions.class);
        globalProcedures.registerFunction(FunctionInjectingTransaction.class);
        globalProcedures.registerFunction(FunctionInjectingKernelTransaction.class);
        globalProcedures.registerFunction(FunctionInjectingDatabase.class);
        globalProcedures.registerFunction(FunctionInjectingSecurityContext.class);
        globalProcedures.registerAggregationFunction(BasicTestAggregationFunctions.class);
    }

    private interface ExecutionContextLogic {

        void doWithExecutionContext(ExecutionContext executionContext) throws ProcedureException;
    }

    private interface ExecutionContextLogic2 {

        void doWithExecutionContext(KernelTransactionImplementation ktx, ExecutionContext executionContext)
                throws ProcedureException;
    }

    public static class BasicTestFunctions {

        @UserFunction("execution.context.test.function.plus")
        public long plus(@Name("value1") long value1, @Name("value2") long value2) {
            return value1 + value2;
        }

        @UserFunction("execution.context.test.function.nodeId")
        public long nodeId(@Name("value") Node value) {
            return value.getId();
        }

        @UserFunction("execution.context.test.function.relationshipId")
        public long relationshipId(@Name("value") Relationship value) {
            return value.getId();
        }

        @UserFunction("execution.context.test.function.nodeIds")
        public List<Long> nodeIds(@Name("value") List<Node> value) {
            return value.stream().map(Entity::getId).collect(Collectors.toList());
        }

        @UserFunction("execution.context.test.function.relationshipIds")
        public List<Long> relationshipIds(@Name("value") List<Relationship> value) {
            return value.stream().map(Entity::getId).collect(Collectors.toList());
        }

        @UserFunction("execution.context.test.function.deleteNode")
        public Object deleteNode(@Name("value") Node value) {
            value.delete();
            return null;
        }

        @UserFunction("execution.context.test.function.deleteRelationship")
        public Object deleteRelationship(@Name("value") Relationship value) {
            value.delete();
            return null;
        }

        @UserFunction("execution.context.test.function.passThrough")
        public Object passThrough(@Name("value") Object value) {
            return value;
        }
    }

    public static class BasicTestAggregationFunctions {

        @UserAggregationFunction("execution.context.test.function.sum")
        public SumAggregationFunction sum() {
            return new SumAggregationFunction();
        }

        @UserAggregationFunction("execution.context.test.function.sumNodeIds")
        public SumNodeIdsAggregationFunction sumNodeIds() {
            return new SumNodeIdsAggregationFunction();
        }
    }

    public static class FunctionInjectingTransaction {

        @Context
        public Transaction transaction;

        @UserFunction("execution.context.test.function.getNodeById")
        public Node getNodeById(@Name("elementId") String elementId) {
            return transaction.getNodeByElementId(elementId);
        }
    }

    public static class FunctionInjectingKernelTransaction {

        @Context
        public KernelTransaction kernelTransaction;

        @UserFunction("execution.context.test.function.doSomethingWithKernelTransaction")
        public Object doSomethingWithKernelTransaction(@Name("value") Object value) {
            kernelTransaction.setMetaData(null);
            return value;
        }
    }

    public static class FunctionInjectingDatabase {

        @Context
        public GraphDatabaseService db;

        @UserFunction("execution.context.test.function.databaseName")
        public String databaseName() {
            return db.databaseName();
        }
    }

    public static class FunctionInjectingSecurityContext {
        @Context
        public SecurityContext securityContext;

        @UserFunction("execution.context.test.function.accessMode")
        public String accessMode() {
            return securityContext.mode().name();
        }
    }

    public static class SumAggregationFunction {

        private long sum = 0;

        @UserAggregationUpdate()
        public void update(@Name("in") long in) {
            sum += in;
        }

        @UserAggregationResult
        public long result() {
            return sum;
        }
    }

    public static class SumNodeIdsAggregationFunction {

        private long sum = 0;

        @UserAggregationUpdate()
        public void update(@Name("in") Node in) {
            sum += in.getId();
        }

        @UserAggregationResult
        public long result() {
            return sum;
        }
    }
}
