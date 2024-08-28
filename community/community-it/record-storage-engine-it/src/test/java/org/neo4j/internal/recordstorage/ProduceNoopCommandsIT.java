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
package org.neo4j.internal.recordstorage;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class ProduceNoopCommandsIT {
    private static final Label LABEL = Label.label("Label");
    private static final Label LABEL2 = Label.label("Label2");
    private static final String KEY = "key";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";
    private static final String KEY4 = "key4";
    private static final String KEY5 = "key5";
    private static final String KEY6 = "key6";
    private static final String KEY7 = "key7";
    private static final String KEY8 = "key8";
    private static final RelationshipType TYPE = RelationshipType.withName("TYPE");
    private static final RelationshipType TYPE2 = RelationshipType.withName("TYPE_2");
    private static final RelationshipType TYPE3 = RelationshipType.withName("TYPE_3");

    @Inject
    private GraphDatabaseAPI db;

    @AfterEach
    void listNoopCommands() throws IOException {
        var txStore = db.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        try (CommandBatchCursor transactions = txStore.getCommandBatches(BASE_TX_ID + 1)) {
            while (transactions.next()) {
                var tx = transactions.get();
                var commands = tx.commandBatch();
                if (hasNoOpCommand(commands)) {
                    StringBuilder error = new StringBuilder("Tx contains no-op commands, " + tx);
                    printNoOpCommands(commands, error);
                    fail(error.toString());
                }
            }
        }
    }

    @Test
    void addExistingLabelToNode() {
        // given
        String id = node().withLabels(LABEL).build();

        // when
        onNode(id, (tx, node) -> node.addLabel(LABEL));
    }

    @Test
    void removeNonExistentLabelFromNode() {
        // given
        String id = node().withLabels(LABEL).build();

        // when
        onNode(id, (tx, node) -> node.removeLabel(LABEL2));
    }

    @Test
    void removeAddLabelToNode() {
        // given
        String id = node().withLabels(LABEL).build();

        // when
        onNode(id, (tx, node) -> {
            node.removeLabel(LABEL);
            node.addLabel(LABEL);
        });
    }

    @Test
    void setNodePropertyToSameValue() {
        // given
        String id = node().withProperty(KEY, 123).build();

        // when
        onNode(id, (tx, node) -> node.setProperty(KEY, 123));
    }

    @Test
    void overwriteNodePropertyInOneEndOfChain() {
        // given
        String id = node().withProperty(KEY, 123)
                .withProperty(KEY2, "123")
                .withProperty(KEY3, 456)
                .withProperty(KEY4, "123")
                .withProperty(KEY5, 789)
                .withProperty(KEY6, "789")
                .withProperty(KEY7, 123456)
                .withProperty(KEY8, "123456")
                .build();

        // when
        onNode(id, (tx, node) -> node.setProperty(KEY6, 123));
    }

    @Test
    void overwriteNodePropertyInAnotherEndOfChain() {
        // given
        String id = node().withProperty(KEY, 123)
                .withProperty(KEY2, "123")
                .withProperty(KEY3, 456)
                .withProperty(KEY4, "123")
                .withProperty(KEY5, 789)
                .withProperty(KEY6, "789")
                .withProperty(KEY7, 123456)
                .withProperty(KEY8, "123456")
                .build();

        // when
        onNode(id, (tx, node) -> node.setProperty(KEY2, 123));
    }

    @Test
    void createRelationshipOnDenseNode() {
        // given
        String id = node().withRelationships(TYPE, 30)
                .withRelationships(TYPE2, 30)
                .withRelationships(TYPE3, 30)
                .build();

        // when
        onNode(id, (tx, node) -> node.createRelationshipTo(tx.createNode(), TYPE2));
    }

    @Test
    void deleteRelationshipFromNode() {
        // given
        String id = node().withRelationships(TYPE, 2)
                .withRelationships(TYPE2, 2)
                .withRelationships(TYPE3, 2)
                .build();

        // when
        onNode(id, (tx, node) -> deleteRelationship(node, 2));
    }

    @Test
    void deleteRelationshipFromDenseNode() {
        // given
        String id = node().withRelationships(TYPE, 30)
                .withRelationships(TYPE2, 30)
                .withRelationships(TYPE3, 30)
                .build();

        // when
        onNode(id, (tx, node) -> deleteRelationship(node, 2));
    }

    @Test
    void createAndDeleteRelationshipOnDenseNode() {
        // given
        String id = node().withRelationships(TYPE, 30)
                .withRelationships(TYPE2, 30)
                .withRelationships(TYPE3, 30)
                .build();

        // when
        onNode(id, (tx, node) -> node.createRelationshipTo(tx.createNode(), TYPE)
                .delete());
    }

    private static void deleteRelationship(Node node, int index) {
        try (ResourceIterable<Relationship> relationships = node.getRelationships();
                ResourceIterator<Relationship> relsIterator = relationships.iterator()) {
            for (int i = 0; i < index - 1; i++) {
                relsIterator.next();
            }
            relsIterator.next().delete();
            while (relsIterator.hasNext()) {
                relsIterator.next();
            }
        }
    }

    private static void printNoOpCommands(CommandBatch commandBatch, StringBuilder error) throws IOException {
        commandBatch.accept(command -> {
            if (command instanceof Command.BaseCommand baseCommand) {
                String toString = baseCommand.toString();
                if (baseCommand.getBefore().equals(baseCommand.getAfter())) {
                    toString += "  <---";
                }
                error.append(format("%n%s", toString));
            }
            return false;
        });
    }

    private static boolean hasNoOpCommand(CommandBatch commandBatch) throws IOException {
        MutableBoolean has = new MutableBoolean();
        commandBatch.accept(command -> {
            if (command instanceof Command.BaseCommand baseCommand) {
                if (baseCommand instanceof Command.PropertyCommand propertyCommand) {
                    fixPropertyRecord(propertyCommand.getBefore());
                    fixPropertyRecord(propertyCommand.getAfter());
                }
                if (baseCommand.getBefore().equals(baseCommand.getAfter())) {
                    has.setTrue();
                }
            }
            return false;
        });
        return has.getValue();
    }

    private static void fixPropertyRecord(PropertyRecord record) {
        for (PropertyBlock block : record.propertyBlocks()) {
            for (long valueBlock : block.getValueBlocks()) {
                record.addLoadedBlock(valueBlock);
            }
        }
    }

    private NodeBuilder node() {
        return new NodeBuilder(db.beginTx());
    }

    private void onNode(String nodeId, BiConsumer<Transaction, Node> action) {
        try (Transaction tx = db.beginTx()) {
            action.accept(tx, tx.getNodeByElementId(nodeId));
            tx.commit();
        }
    }

    private static class NodeBuilder {
        private final Transaction tx;
        private final Node node;

        NodeBuilder(Transaction tx) {
            this.tx = tx;
            this.node = tx.createNode();
        }

        NodeBuilder withLabels(Label... labels) {
            Stream.of(labels).forEach(node::addLabel);
            return this;
        }

        NodeBuilder withRelationships(RelationshipType type, int count) {
            for (int i = 0; i < count; i++) {
                node.createRelationshipTo(tx.createNode(), type);
            }
            return this;
        }

        NodeBuilder withProperty(String key, Object value) {
            node.setProperty(key, value);
            return this;
        }

        String build() {
            tx.commit();
            tx.close();
            return node.getElementId();
        }
    }
}
