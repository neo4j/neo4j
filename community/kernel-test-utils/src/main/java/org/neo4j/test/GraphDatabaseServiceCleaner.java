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
package org.neo4j.test;

import static org.neo4j.internal.helpers.collection.Iterators.loop;

import java.util.concurrent.TimeUnit;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class GraphDatabaseServiceCleaner {
    private GraphDatabaseServiceCleaner() {
        throw new UnsupportedOperationException();
    }

    public static void cleanDatabaseContent(GraphDatabaseService db) {
        cleanupSchema(db);
        cleanupAllRelationshipsAndNodes(db);
    }

    public static void cleanupSchema(GraphDatabaseService db) {
        try (InternalTransaction tx =
                ((GraphDatabaseAPI) db).beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            for (ConstraintDescriptor constraintDescriptor :
                    loop(tx.kernelTransaction().schemaRead().constraintsGetAll())) {
                tx.kernelTransaction().schemaWrite().constraintDrop(constraintDescriptor, true);
            }
            for (IndexDefinition index : tx.schema().getIndexes()) {
                index.drop();
            }
            tx.commit();
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
        // re-create the default indexes
        try (Transaction tx = db.beginTx()) {
            tx.schema()
                    .indexFor(AnyTokens.ANY_RELATIONSHIP_TYPES)
                    .withName("rti")
                    .create();
            tx.schema().indexFor(AnyTokens.ANY_LABELS).withName("lti").create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.MINUTES);
        }
    }

    public static void cleanupAllRelationshipsAndNodes(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx();
                ResourceIterable<Relationship> allRelationships = tx.getAllRelationships();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            allRelationships.forEach(Relationship::delete);
            allNodes.forEach(Node::delete);
            tx.commit();
        }
    }
}
