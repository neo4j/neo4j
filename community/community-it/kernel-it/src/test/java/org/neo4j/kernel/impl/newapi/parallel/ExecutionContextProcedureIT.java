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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.security.AccessMode.Static.FULL;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.virtual.VirtualValues.relationship;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.collection.RawIterator;
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
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.kernel.impl.util.PathWrappingPathValue;
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.NodeIdReference;
import org.neo4j.values.virtual.PathReference;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.VirtualValues;

@DbmsExtension
class ExecutionContextProcedureIT {

    private static final String RUNTIME_USED = "TEST";

    @Inject
    private GraphDatabaseAPI db;

    @BeforeEach
    void beforeEach() throws KernelException {
        registerProcedures();
    }

    @Test
    void testProcedureAcceptingBasicType() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            List<AnyValue[]> result = invokeProcedure(executionContext, "range", intValue(0), intValue(3));
            assertColumnCount(result, 1);
            assertThat(result.stream().map(row -> row[0])).containsExactly(intValue(0), intValue(1), intValue(2));
        });
    }

    @ValueSource(strings = {"relationshipsWithCompilation", "relationshipsWithoutCompilation"})
    @ParameterizedTest
    void testProcedureAcceptingNodeAndProducingRelationships(String method) throws ProcedureException {
        NodeIdReference nodeReference;
        List<RelationshipReference> relReferences = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            var node = tx.createNode();
            nodeReference = VirtualValues.node(node.getId());
            relReferences.add(
                    relationship(node.createRelationshipTo(node, withName("T1")).getId()));
            relReferences.add(
                    relationship(node.createRelationshipTo(node, withName("T2")).getId()));
            relReferences.add(
                    relationship(node.createRelationshipTo(node, withName("T3")).getId()));
            tx.commit();
        }

        doWithExecutionContext(executionContext -> {
            List<AnyValue[]> result = invokeProcedure(executionContext, method, nodeReference);
            assertColumnCount(result, 1);
            assertThat(result.stream().map(row -> row[0])).containsExactlyInAnyOrderElementsOf(relReferences);
        });
    }

    @ValueSource(strings = {"nodesWithCompilation", "nodesWithoutCompilation"})
    @ParameterizedTest
    void testProcedureAcceptingRelationshipAndProducingNodes(String method) throws ProcedureException {
        RelationshipReference relReference;
        NodeIdReference startNodeReference;
        NodeIdReference endNodeReference;
        try (Transaction tx = db.beginTx()) {
            var startNode = tx.createNode();
            startNodeReference = VirtualValues.node(startNode.getId());
            var endNode = tx.createNode();
            endNodeReference = VirtualValues.node(endNode.getId());
            relReference = relationship(
                    startNode.createRelationshipTo(endNode, withName("T1")).getId());
            tx.commit();
        }

        doWithExecutionContext(executionContext -> {
            List<AnyValue[]> result = invokeProcedure(executionContext, method, relReference);
            assertColumnCount(result, 1);
            assertThat(result.stream().map(row -> row[0])).containsExactly(startNodeReference, endNodeReference);
        });
    }

    @ValueSource(strings = {"passThrough", "passThroughPath"})
    @ParameterizedTest
    void testProcedureAcceptingPathAndProducingPath(String method) throws ProcedureException {
        long[] nodeIds = new long[3];
        long[] relIds = new long[2];
        try (Transaction tx = db.beginTx()) {
            var node1 = tx.createNode();
            nodeIds[0] = node1.getId();
            var node2 = tx.createNode();
            nodeIds[1] = node2.getId();
            var node3 = tx.createNode();
            nodeIds[2] = node3.getId();
            relIds[0] = node1.createRelationshipTo(node2, withName("T1")).getId();
            relIds[1] = node2.createRelationshipTo(node3, withName("T1")).getId();
            tx.commit();
        }

        var path = VirtualValues.pathReference(nodeIds, relIds);

        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeSimpleProcedure(executionContext, method, path);
            assertThat(result).isInstanceOf(PathReference.class);
            PathReference resultPath = (PathReference) result;
            assertThat(resultPath.nodeIds()).containsExactly(nodeIds);
            assertThat(resultPath.relationshipIds()).containsExactly(relIds);
        });
    }

    @Test
    void procedureShouldNotWrapExecutionContextNodes() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeSimpleProcedure(executionContext, "passThrough", VirtualValues.node(123));
            assertThat(result).isNotInstanceOf(NodeEntityWrappingNodeValue.class);
        });
    }

    @Test
    void procedureShouldNotWrapExecutionContextRelationships() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeSimpleProcedure(executionContext, "passThrough", relationship(123));
            assertThat(result).isNotInstanceOf(RelationshipEntityWrappingValue.class);
        });
    }

    @Test
    void procedureShouldNotHandleWrappedNodesAsReferences() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var originalWrappedNode = mock(Node.class);
            AnyValue result = invokeSimpleProcedure(
                    executionContext, "passThrough", ValueUtils.wrapNodeEntity(originalWrappedNode));
            assertThat(result).isInstanceOf(NodeEntityWrappingNodeValue.class);
            var unwrappedNode = ((NodeEntityWrappingNodeValue) result).getEntity();
            assertThat(unwrappedNode).isEqualTo(originalWrappedNode);
        });
    }

    @Test
    void procedureShouldNotHandleWrappedRelationshipsAsReferences() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var originalWrappedRelationship = mock(Relationship.class);
            AnyValue result = invokeSimpleProcedure(
                    executionContext, "passThrough", ValueUtils.wrapRelationshipEntity(originalWrappedRelationship));
            assertThat(result).isInstanceOf(RelationshipEntityWrappingValue.class);
            var unwrappedRelationship = ((RelationshipEntityWrappingValue) result).getEntity();
            assertThat(unwrappedRelationship).isEqualTo(originalWrappedRelationship);
        });
    }

    @Test
    void procedureShouldNotHandleWrappedPathsAsReferences() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var originalWrappedPath = mock(Path.class);
            AnyValue result =
                    invokeSimpleProcedure(executionContext, "passThrough", ValueUtils.wrapPath(originalWrappedPath));
            assertThat(result).isInstanceOf(PathWrappingPathValue.class);
            var unwrappedPath = ((PathWrappingPathValue) result).path();
            assertThat(unwrappedPath).isEqualTo(originalWrappedPath);
        });
    }

    @Test
    void testProcedureUsingUnsupportedNodeOperation() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            assertThatThrownBy(() -> invokeProcedure(executionContext, "deleteEntity", VirtualValues.node(123)))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Operation unsupported during parallel query execution");
        });
    }

    @Test
    void testProcedureUsingUnsupportedRelationshipOperation() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            assertThatThrownBy(() -> invokeProcedure(executionContext, "deleteEntity", relationship(123)))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Operation unsupported during parallel query execution");
        });
    }

    @Test
    void testInjectingTransactionIntoProcedureAndGettingDataFromIt() throws ProcedureException {
        NodeIdReference node1;
        NodeIdReference node2;
        try (Transaction tx = db.beginTx()) {
            node1 = VirtualValues.node(tx.createNode().getId());
            node2 = VirtualValues.node(tx.createNode().getId());
            tx.commit();
        }

        doWithExecutionContext(executionContext -> {
            List<AnyValue[]> result = invokeProcedure(executionContext, "getAllNodesFromTransaction");
            assertColumnCount(result, 1);
            assertThat(result.stream().map(row -> row[0])).containsExactly(node1, node2);
        });
    }

    @Test
    void testKernelTransactionInjectionIntoProcedure() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            assertThatThrownBy(() -> invokeProcedure(executionContext, "doSomethingWithKernelTransaction"))
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "`transaction.createExecutionContext' is not supported in procedures when called from parallel runtime. Please retry using another runtime.");
        });
    }

    @Test
    void testGraphDatabaseServiceInjectionIntoProcedure() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            AnyValue result = invokeSimpleProcedure(executionContext, "databaseName");
            assertThat(result).isEqualTo(Values.stringValue(db.databaseName()));
        });
    }

    @Test
    void testProcedureSecurityContext() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            // We should start with FULL access mode ...
            AccessMode originalAccessMode = executionContext.securityContext().mode();
            assertThat(originalAccessMode).isEqualTo(FULL);

            // ... which should be restricted to READ during the procedure call ...
            AnyValue result = invokeSimpleProcedure(executionContext, "accessMode");
            assertThat(result)
                    .isEqualTo(Values.stringValue(
                            new OverriddenAccessMode(originalAccessMode, AccessMode.Static.READ).name()));

            // ... and restored to FULL again after the call.
            assertThat(executionContext.securityContext().mode()).isEqualTo(FULL);
        });
    }

    @Test
    void closedTransactionShouldBeDetectedOnProcedureInvocation() throws ProcedureException {
        try (Transaction transaction = db.beginTx();
                Statement statement = acquireStatement(transaction);
                ExecutionContext executionContext = createExecutionContext(transaction)) {
            try {
                var handle = executionContext.procedures().procedureGet(getName("range"), CypherScope.CYPHER_5);
                transaction.rollback();
                var procContext =
                        new ProcedureCallContext(handle.id(), EMPTY_STRING_ARRAY, false, "", false, RUNTIME_USED);
                assertThatThrownBy(() -> executionContext
                                .procedures()
                                .procedureCallRead(handle.id(), new Value[] {intValue(0), intValue(2)}, procContext))
                        .isInstanceOf(NotInTransactionException.class)
                        .hasMessageContaining("This transaction has already been closed.");
            } finally {
                executionContext.complete();
            }
        }
    }

    @Test
    void testProcedureWithWriteAccessMode() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var handle = executionContext.procedures().procedureGet(getName("range"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), EMPTY_STRING_ARRAY, false, "", false, RUNTIME_USED);
            assertThatThrownBy(() -> executionContext
                            .procedures()
                            .procedureCallWrite(handle.id(), new Value[] {intValue(0), intValue(2)}, procContext))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "Invoking procedure with WRITE access mode is not allowed during parallel execution.");
        });
    }

    @Test
    void testProcedureWithSchemaAccessMode() throws ProcedureException {
        doWithExecutionContext(executionContext -> {
            var handle = executionContext.procedures().procedureGet(getName("range"), CypherScope.CYPHER_5);
            var procContext = new ProcedureCallContext(handle.id(), EMPTY_STRING_ARRAY, false, "", false, RUNTIME_USED);
            assertThatThrownBy(() -> executionContext
                            .procedures()
                            .procedureCallSchema(handle.id(), new Value[] {intValue(0), intValue(2)}, procContext))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "Invoking procedure with SCHEMA access mode is not allowed during parallel execution.");
        });
    }

    private void assertColumnCount(List<AnyValue[]> result, int count) {
        result.forEach(row -> assertThat(row.length).isEqualTo(count));
    }

    private void registerProcedures() throws KernelException {
        var globalProcedures = db.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        globalProcedures.registerProcedure(BasicTestProcedures.class);
        globalProcedures.registerProcedure(ProcedureInjectingTransaction.class);
        globalProcedures.registerProcedure(ProcedureInjectingKernelTransaction.class);
        globalProcedures.registerProcedure(ProcedureInjectingDatabase.class);
        globalProcedures.registerProcedure(ProcedureInjectingSecurityContext.class);
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

    private ExecutionContext createExecutionContext(Transaction transaction) {
        return ((InternalTransaction) transaction).kernelTransaction().createExecutionContext();
    }

    private Statement acquireStatement(Transaction transaction) {
        return ((InternalTransaction) transaction).kernelTransaction().acquireStatement();
    }

    private List<AnyValue[]> invokeProcedure(ExecutionContext executionContext, String name, AnyValue... args)
            throws ProcedureException {
        var handle = executionContext.procedures().procedureGet(getName(name), CypherScope.CYPHER_5);
        var procContext = new ProcedureCallContext(handle.id(), EMPTY_STRING_ARRAY, false, "", false, RUNTIME_USED);
        RawIterator<AnyValue[], ProcedureException> iterator =
                executionContext.procedures().procedureCallRead(handle.id(), args, procContext);
        var result = new ArrayList<AnyValue[]>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        return result;
    }

    // A shortcut for invoking a procedure returning one row with one column.
    private AnyValue invokeSimpleProcedure(ExecutionContext executionContext, String name, AnyValue... args)
            throws ProcedureException {
        return invokeProcedure(executionContext, name, args).stream()
                .map(row -> row[0])
                .findFirst()
                .get();
    }

    private QualifiedName getName(String name) {
        return new QualifiedName("execution", "context", "test", "procedure", name);
    }

    private interface ExecutionContextLogic {

        void doWithExecutionContext(ExecutionContext executionContext) throws ProcedureException;
    }

    public record GenericResult(Object object) {}

    public record NodeResult(Node node) {}

    public static class BasicTestProcedures {

        @Procedure("execution.context.test.procedure.range")
        public Stream<GenericResult> range(@Name("from") long from, @Name("to") long to) {
            return LongStream.range(from, to).mapToObj(GenericResult::new);
        }

        // The difference between the two relationships* procedures is that the type conversion will be "compiled"
        // into the procedure if the types are clear upon loading. So the code paths will be quite different,
        // just because the procedure compiler cannot reason about types up front in the second case.
        @Procedure("execution.context.test.procedure.relationshipsWithCompilation")
        public Stream<RelationshipResult> relationshipsWithCompilation(@Name("node") Node node) {
            return node.getRelationships().stream().map(RelationshipResult::new);
        }

        @Procedure("execution.context.test.procedure.relationshipsWithoutCompilation")
        public Stream<GenericResult> relationshipsWithoutCompilation(@Name("node") Object o) {
            if (o instanceof Node node) {
                return node.getRelationships().stream().map(GenericResult::new);
            }
            throw new IllegalStateException("Procedures accepts only one Node instance");
        }

        @Procedure("execution.context.test.procedure.nodesWithCompilation")
        public Stream<NodeResult> nodesWithCompilation(@Name("relationship") Relationship relationship) {
            return Stream.of(relationship.getStartNode(), relationship.getEndNode())
                    .map(NodeResult::new);
        }

        @Procedure("execution.context.test.procedure.nodesWithoutCompilation")
        public Stream<GenericResult> nodesWithoutCompilation(@Name("relationship") Object o) {
            if (o instanceof Relationship relationship) {
                return Stream.of(relationship.getStartNode(), relationship.getEndNode())
                        .map(GenericResult::new);
            }
            throw new IllegalStateException("Procedures accepts only one Relationship instance");
        }

        @Procedure("execution.context.test.procedure.passThrough")
        public Stream<GenericResult> passThrough(@Name("Object") Object o) {
            return Stream.of(o).map(GenericResult::new);
        }

        @Procedure("execution.context.test.procedure.passThroughPath")
        public Stream<PathResult> passThroughPath(@Name("Object") Path path) {
            return Stream.of(path).map(PathResult::new);
        }

        @Procedure("execution.context.test.procedure.deleteEntity")
        public void deleteEntity(@Name("entity") Object o) {
            if (o instanceof Entity entity) {
                entity.delete();
            } else {
                throw new IllegalStateException("Procedures accepts only one Entity instance");
            }
        }

        public record RelationshipResult(Relationship relationship) {}

        public record PathResult(Path path) {}
    }

    public static class ProcedureInjectingTransaction {

        @Context
        public Transaction transaction;

        @Procedure("execution.context.test.procedure.getAllNodesFromTransaction")
        public Stream<NodeResult> getAllNodesFromTransaction() {
            return transaction.getAllNodes().stream().map(NodeResult::new);
        }
    }

    public static class ProcedureInjectingKernelTransaction {

        @Context
        public KernelTransaction kernelTransaction;

        @NotThreadSafe
        @Procedure("execution.context.test.procedure.doSomethingWithKernelTransaction")
        public void doSomethingWithKernelTransaction() {
            kernelTransaction.createExecutionContext();
        }
    }

    public static class ProcedureInjectingDatabase {

        @Context
        public GraphDatabaseService db;

        @Procedure("execution.context.test.procedure.databaseName")
        public Stream<GenericResult> databaseName() {
            return Stream.of(db.databaseName()).map(GenericResult::new);
        }
    }

    public static class ProcedureInjectingSecurityContext {
        @Context
        public SecurityContext securityContext;

        @Procedure("execution.context.test.procedure.accessMode")
        public Stream<GenericResult> accessMode() {
            return Stream.of(securityContext.mode().name()).map(GenericResult::new);
        }
    }
}
