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
package org.neo4j.kernel.api.impl.fulltext;

import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.FulltextSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

class FulltextAnalyzerTest extends LuceneFulltextTestSupport {
    private static final String ENGLISH = "english";
    static final String SWEDISH = "swedish";
    private static final String FOLDING = "standard-folding";
    public static final String NODE_INDEX_NAME = "nodes";
    public static final String REL_INDEX_NAME = "rels";

    @Test
    void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception {
        applySetting(FulltextSettings.fulltext_default_analyzer, ENGLISH);

        createIndexes();

        long nodeId;
        long relId;
        try (Transaction tx = db.beginTx()) {
            createNodeIndexableByPropertyValue(tx, LABEL, "Hello and hello again, in the end.");
            nodeId = createNodeIndexableByPropertyValue(tx, LABEL, "En apa och en tomte bodde i ett hus.");

            createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "Hello and hello again, in the end.");
            relId = createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "En apa och en tomte bodde i ett hus.");
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "and");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "in");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "the");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "and");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "in");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "the");
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "en", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "och", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "ett", nodeId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "en", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "och", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "ett", relId);
        }
    }

    @Test
    void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception {
        applySetting(FulltextSettings.fulltext_default_analyzer, SWEDISH);

        createIndexes();

        long nodeId;
        long relId;
        try (Transaction tx = db.beginTx()) {
            nodeId = createNodeIndexableByPropertyValue(tx, LABEL, "Hello and hello again, in the end.");
            createNodeIndexableByPropertyValue(tx, LABEL, "En apa och en tomte bodde i ett hus.");

            relId = createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "Hello and hello again, in the end.");
            createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "En apa och en tomte bodde i ett hus.");

            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "and", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "in", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "the", nodeId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "and", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "in", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "the", relId);
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "en");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "och");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "ett");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "en");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "och");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "ett");
        }
    }

    @Test
    void shouldBeAbleToSpecifyFoldingAnalyzer() throws Exception {
        applySetting(FulltextSettings.fulltext_default_analyzer, FOLDING);

        createIndexes();

        long nodeId;
        long nodeId2;
        long nodeId3;
        long relId;
        long relId2;
        long relId3;
        try (Transaction tx = db.beginTx()) {
            nodeId = createNodeIndexableByPropertyValue(tx, LABEL, "Příliš žluťoučký kůň úpěl ďábelské ódy.");
            nodeId2 = createNodeIndexableByPropertyValue(tx, LABEL, "1SOMEDATA1");
            nodeId3 = createNodeIndexableByPropertyValue(tx, LABEL, "Ⓐpa Ɐmma Ǣta Ꜷajaj Ꜻverka dett⒜");

            relId = createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "Příliš žluťoučký kůň úpěl ďábelské ódy.");
            relId2 = createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "1SOMEDATA1");
            relId3 = createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "Ⓐpa Ɐmma Ǣta Ꜷajaj Ꜻverka dett⒜");

            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "prilis", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "zlutoucky", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "kun", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "upel", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "dabelske", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "ody", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "1SOMEDATA1", nodeId2);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "1somedata1", nodeId2);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "*SOMEDATA*", nodeId2);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "*somedata*", nodeId2);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "Apa", nodeId3);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "amma", nodeId3);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "AEta", nodeId3);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "Auajaj", nodeId3);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "Avverka", nodeId3);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "dett(a)", nodeId3);

            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "prilis", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "zlutoucky", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "kun", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "upel", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "dabelske", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "ody", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "1SOMEDATA1", relId2);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "1somedata1", relId2);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "*SOMEDATA*", relId2);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "*somedata*", relId2);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "Apa", relId3);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "amma", relId3);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "AEta", relId3);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "Auajaj", relId3);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "Avverka", relId3);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "dett(a)", relId3);
        }
    }

    @Test
    void shouldNotReindexNodesWhenDefaultAnalyzerIsChanged() throws Exception {
        applySetting(FulltextSettings.fulltext_default_analyzer, ENGLISH);

        createIndexes();

        long nodeId;
        long relId;
        try (Transaction tx = db.beginTx()) {
            createNodeIndexableByPropertyValue(tx, LABEL, "Hello and hello again, in the end.");
            nodeId = createNodeIndexableByPropertyValue(tx, LABEL, "En apa och en tomte bodde i ett hus.");

            createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "Hello and hello again, in the end.");
            relId = createRelationshipIndexableByPropertyValue(
                    tx, tx.createNode().getId(), tx.createNode().getId(), "En apa och en tomte bodde i ett hus.");
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = kernelTransaction(tx);
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "and");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "in");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "the");
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "en", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "och", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "ett", nodeId);

            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "and");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "in");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "the");
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "en", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "och", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "ett", relId);
        }

        applySetting(FulltextSettings.fulltext_default_analyzer, SWEDISH);
        try (KernelTransactionImplementation ktx = getKernelTransaction()) {
            SchemaRead schemaRead = ktx.schemaRead();
            await(schemaRead.indexGetForName(NODE_INDEX_NAME));
            // These results should be exactly the same as before the configuration change and restart.
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "and");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "in");
            assertQueryFindsNothing(ktx, true, NODE_INDEX_NAME, "the");
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "en", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "och", nodeId);
            assertQueryFindsIds(ktx, true, NODE_INDEX_NAME, "ett", nodeId);

            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "and");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "in");
            assertQueryFindsNothing(ktx, false, REL_INDEX_NAME, "the");
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "en", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "och", relId);
            assertQueryFindsIds(ktx, false, REL_INDEX_NAME, "ett", relId);
        }
    }

    private void createIndexes() {
        try (var tx = db.beginTx()) {
            tx.schema()
                    .indexFor(LABEL)
                    .on(PROP)
                    .withIndexType(FULLTEXT)
                    .withName(NODE_INDEX_NAME)
                    .create();
            tx.schema()
                    .indexFor(RELTYPE)
                    .on(PROP)
                    .withIndexType(FULLTEXT)
                    .withName(REL_INDEX_NAME)
                    .create();
            tx.commit();
        }
        try (var tx = db.beginTx()) {
            tx.schema().awaitIndexOnline(NODE_INDEX_NAME, 1, TimeUnit.MINUTES);
            tx.schema().awaitIndexOnline(REL_INDEX_NAME, 1, TimeUnit.MINUTES);
        }
    }
}
