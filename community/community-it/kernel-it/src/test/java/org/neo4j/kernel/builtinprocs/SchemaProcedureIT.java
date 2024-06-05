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
package org.neo4j.kernel.builtinprocs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

import org.junit.jupiter.api.Test;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

class SchemaProcedureIT extends KernelIntegrationTest {
    @Test
    void testEmptyGraph() throws Throwable {
        // Given the database is empty

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "schema", "visualization"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            assertThat(asList(stream)).containsExactly(new AnyValue[] {EMPTY_LIST, EMPTY_LIST});
        }
    }

    @Test
    void testLabelIndex() throws Throwable {
        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        long nbrIndexesOnStartup = Iterators.count(transaction.schemaRead().indexesGetAll());

        // Given there is label with index and a constraint
        long nodeId = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName("Person");
        transaction.dataWrite().nodeAddLabel(nodeId, labelId);
        int propertyIdName = transaction.tokenWrite().propertyKeyGetOrCreateForName("name");
        int propertyIdAge = transaction.tokenWrite().propertyKeyGetOrCreateForName("age");
        transaction.dataWrite().nodeSetProperty(nodeId, propertyIdName, Values.of("Emil"));
        commit();

        SchemaWrite schemaOps = schemaWriteInNewTransaction();
        schemaOps.indexCreate(
                IndexPrototype.forSchema(forLabel(labelId, propertyIdName)).withName("my index"));
        var constraintName = "constraint name";
        schemaOps.uniquePropertyConstraintCreate(
                uniqueForSchema(forLabel(labelId, propertyIdAge)).withName(constraintName));
        commit();

        // When
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "schema", "visualization"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            while (stream.hasNext()) {
                AnyValue[] next = stream.next();
                assertEquals(2, next.length);
                ListValue nodes = (ListValue) next[0];
                assertEquals(1, nodes.size());
                NodeValue node = (NodeValue) nodes.value(0);
                assertThat(node.labels()).isEqualTo(Values.stringArray("Person"));
                assertEquals(stringValue("Person"), node.properties().get("name"));
                assertEquals(
                        VirtualValues.list(stringValue("name")),
                        node.properties().get("indexes"));
                assertEquals(
                        VirtualValues.list(stringValue("Constraint( id=" + constraintId(constraintName)
                                + ", name='constraint name', type='UNIQUENESS', schema=(:Person {age}), "
                                + "ownedIndex="
                                + indexId(constraintName) + " )")),
                        node.properties().get("constraints"));
            }
        }
    }

    @Test
    void testRelationship() throws Throwable {
        // Given there ar
        KernelTransaction transaction = newTransaction(AnonymousContext.writeToken());
        long nodeIdPerson = transaction.dataWrite().nodeCreate();
        int labelIdPerson = transaction.tokenWrite().labelGetOrCreateForName("Person");
        transaction.dataWrite().nodeAddLabel(nodeIdPerson, labelIdPerson);
        long nodeIdLocation = transaction.dataWrite().nodeCreate();
        int labelIdLocation = transaction.tokenWrite().labelGetOrCreateForName("Location");
        transaction.dataWrite().nodeAddLabel(nodeIdLocation, labelIdLocation);
        int relationshipTypeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName("LIVES_IN");
        transaction.dataWrite().relationshipCreate(nodeIdPerson, relationshipTypeId, nodeIdLocation);
        commit();

        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            RawIterator<AnyValue[], ProcedureException> stream = procs.procedureCallRead(
                    procs.procedureGet(procedureName("db", "schema", "visualization"), CypherScope.CYPHER_5)
                            .id(),
                    new AnyValue[0],
                    ProcedureCallContext.EMPTY);

            // Then
            while (stream.hasNext()) {
                AnyValue[] next = stream.next();
                assertEquals(2, next.length);
                ListValue relationships = (ListValue) next[1];
                assertEquals(1, relationships.size());
                RelationshipValue relationship = (RelationshipValue) relationships.value(0);
                assertEquals("LIVES_IN", relationship.type().stringValue());
                NodeValue start = (NodeValue) relationship.startNode();
                NodeValue end = (NodeValue) relationship.endNode();
                assertThat(start.labels()).isEqualTo(Values.stringArray("Person"));
                assertThat(end.labels()).isEqualTo(Values.stringArray("Location"));
            }
        }
    }

    private long constraintId(String name) throws TransactionFailureException {
        try (var tx = newTransaction()) {
            return tx.schemaRead().constraintGetForName(name).getId();
        }
    }

    private long indexId(String name) throws TransactionFailureException {
        try (var tx = newTransaction()) {
            return tx.schemaRead().indexGetForName(name).getId();
        }
    }
}
