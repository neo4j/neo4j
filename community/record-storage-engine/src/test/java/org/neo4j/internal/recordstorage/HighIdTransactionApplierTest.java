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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class HighIdTransactionApplierTest {
    private static final IndexProviderDescriptor PROVIDER_DESCRIPTOR = new IndexProviderDescriptor("empty", "1");
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    @Inject
    private PageCache pageCache;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;

    @BeforeEach
    void before() {
        var pageCacheTracer = PageCacheTracer.NULL;
        var storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        neoStores = storeFactory.openAllNeoStores();
    }

    @AfterEach
    void after() {
        neoStores.close();
    }

    @Test
    void shouldUpdateHighIdsOnExternalTransaction() {
        // GIVEN
        HighIdTransactionApplier tracker = new HighIdTransactionApplier(neoStores);

        // WHEN
        // Nodes
        tracker.visitNodeCommand(Commands.createNode(10, 2, 3));
        tracker.visitNodeCommand(Commands.createNode(20, 4, 5));

        // Relationships
        tracker.visitRelationshipCommand(Commands.createRelationship(4, 10, 20, 0));
        tracker.visitRelationshipCommand(Commands.createRelationship(45, 10, 20, 1));

        // Label tokens
        tracker.visitLabelTokenCommand(Commands.createLabelToken(3, 0));
        tracker.visitLabelTokenCommand(Commands.createLabelToken(5, 1));

        // Property tokens
        tracker.visitPropertyKeyTokenCommand(Commands.createPropertyKeyToken(3, 0));
        tracker.visitPropertyKeyTokenCommand(Commands.createPropertyKeyToken(5, 1));

        // Relationship type tokens
        tracker.visitRelationshipTypeTokenCommand(Commands.createRelationshipTypeToken(3, 0));
        tracker.visitRelationshipTypeTokenCommand(Commands.createRelationshipTypeToken(5, 1));

        // Relationship groups
        tracker.visitRelationshipGroupCommand(Commands.createRelationshipGroup(10, 1));
        tracker.visitRelationshipGroupCommand(Commands.createRelationshipGroup(20, 2));

        // Schema rules
        tracker.visitSchemaRuleCommand(
                Commands.createIndexRule(PROVIDER_DESCRIPTOR, 10, SchemaDescriptors.forLabel(0, 1)));
        tracker.visitSchemaRuleCommand(
                Commands.createIndexRule(PROVIDER_DESCRIPTOR, 20, SchemaDescriptors.forLabel(1, 2)));

        // Properties
        tracker.visitPropertyCommand(Commands.createProperty(10, PropertyType.STRING, 0, 6, 7));
        tracker.visitPropertyCommand(Commands.createProperty(20, PropertyType.ARRAY, 1, 8, 9));

        tracker.close();

        // THEN
        assertEquals(20 + 1, neoStores.getNodeStore().getIdGenerator().getHighId(), "NodeStore");
        assertEquals(
                5 + 1,
                neoStores.getNodeStore().getDynamicLabelStore().getIdGenerator().getHighId(),
                "DynamicNodeLabelStore");
        assertEquals(45 + 1, neoStores.getRelationshipStore().getIdGenerator().getHighId(), "RelationshipStore");
        assertEquals(
                5 + 1,
                neoStores.getRelationshipTypeTokenStore().getIdGenerator().getHighId(),
                "RelationshipTypeStore");
        assertEquals(
                1 + 1,
                neoStores
                        .getRelationshipTypeTokenStore()
                        .getNameStore()
                        .getIdGenerator()
                        .getHighId(),
                "RelationshipType NameStore");
        assertEquals(
                5 + 1, neoStores.getPropertyKeyTokenStore().getIdGenerator().getHighId(), "PropertyKeyStore");
        assertEquals(
                1 + 1,
                neoStores
                        .getPropertyKeyTokenStore()
                        .getNameStore()
                        .getIdGenerator()
                        .getHighId(),
                "PropertyKey NameStore");
        assertEquals(5 + 1, neoStores.getLabelTokenStore().getIdGenerator().getHighId(), "LabelStore");
        assertEquals(
                1 + 1,
                neoStores.getLabelTokenStore().getNameStore().getIdGenerator().getHighId(),
                "Label NameStore");
        assertEquals(20 + 1, neoStores.getPropertyStore().getIdGenerator().getHighId(), "PropertyStore");
        assertEquals(
                7 + 1,
                neoStores.getPropertyStore().getStringStore().getIdGenerator().getHighId(),
                "PropertyStore DynamicStringStore");
        assertEquals(
                9 + 1,
                neoStores.getPropertyStore().getArrayStore().getIdGenerator().getHighId(),
                "PropertyStore DynamicArrayStore");
        assertEquals(20 + 1, neoStores.getSchemaStore().getIdGenerator().getHighId(), "SchemaStore");
    }

    @Test
    void shouldTrackSecondaryUnitIdsAsWell() {
        // GIVEN
        HighIdTransactionApplier tracker = new HighIdTransactionApplier(neoStores);

        NodeRecord node = new NodeRecord(5).initialize(true, 123, true, 456, 0);
        node.setSecondaryUnitIdOnLoad(6);

        RelationshipRecord relationship =
                new RelationshipRecord(10).initialize(true, 1, 2, 3, 4, 5, 6, 7, 8, true, true);
        relationship.setSecondaryUnitIdOnLoad(12);

        RelationshipGroupRecord relationshipGroup = new RelationshipGroupRecord(8).initialize(true, 0, 1, 2, 3, 4, 5);
        relationshipGroup.setSecondaryUnitIdOnLoad(20);

        // WHEN
        tracker.visitNodeCommand(new NodeCommand(LATEST_LOG_SERIALIZATION, new NodeRecord(node.getId()), node));
        tracker.visitRelationshipCommand(new RelationshipCommand(
                LATEST_LOG_SERIALIZATION, new RelationshipRecord(relationship.getId()), relationship));
        tracker.visitRelationshipGroupCommand(new RelationshipGroupCommand(
                LATEST_LOG_SERIALIZATION, new RelationshipGroupRecord(relationshipGroup.getId()), relationshipGroup));
        tracker.close();

        // THEN
        assertEquals(
                node.getSecondaryUnitId() + 1,
                neoStores.getNodeStore().getIdGenerator().getHighId());
        assertEquals(
                relationship.getSecondaryUnitId() + 1,
                neoStores.getRelationshipStore().getIdGenerator().getHighId());
        assertEquals(
                relationshipGroup.getSecondaryUnitId() + 1,
                neoStores.getRelationshipGroupStore().getIdGenerator().getHighId());
    }
}
