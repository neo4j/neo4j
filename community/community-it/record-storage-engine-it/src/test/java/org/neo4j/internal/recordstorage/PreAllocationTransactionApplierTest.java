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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.LatestVersions;

class PreAllocationTransactionApplierTest {
    private static final IndexProviderDescriptor PROVIDER_DESCRIPTOR = new IndexProviderDescriptor("empty", "1");
    private static final LogCommandSerialization LATEST_LOG_SERIALIZATION =
            RecordStorageCommandReaderFactory.INSTANCE.get(LatestVersions.LATEST_KERNEL_VERSION);

    private NeoStores neoStores;
    private NodeStore fakeNodeStore;
    private RelationshipStore fakeRelStore;
    private DynamicArrayStore fakeDynamicLabelStore;
    private LabelTokenStore fakeLabelTokenStore;
    private DynamicStringStore fakeLabelTokenNameStore;
    private PropertyKeyTokenStore fakePropKeyStore;
    private DynamicStringStore fakePropKeyNameStore;
    private RelationshipTypeTokenStore fakeRelTypeTokenStore;
    private DynamicStringStore fakeRelTypeNameStore;
    private RelationshipGroupStore fakeRelGroupStore;
    private SchemaStore fakeSchemaStore;
    private PropertyStore fakePropertyStore;
    private DynamicStringStore fakePropertyStringStore;
    private DynamicArrayStore fakePropertyArrayStore;
    private MetaDataStore fakeMetaDataStore;

    @BeforeEach
    void before() {
        neoStores = mock(NeoStores.class);
        fakeNodeStore = mock(NodeStore.class);
        fakeDynamicLabelStore = mock(DynamicArrayStore.class);
        fakeRelStore = mock(RelationshipStore.class);
        fakeLabelTokenStore = mock(LabelTokenStore.class);
        fakeLabelTokenNameStore = mock(DynamicStringStore.class);
        fakePropKeyStore = mock(PropertyKeyTokenStore.class);
        fakePropKeyNameStore = mock(DynamicStringStore.class);
        fakeRelTypeTokenStore = mock(RelationshipTypeTokenStore.class);
        fakeRelTypeNameStore = mock(DynamicStringStore.class);
        fakeRelGroupStore = mock(RelationshipGroupStore.class);
        fakeSchemaStore = mock(SchemaStore.class);
        fakePropertyStore = mock(PropertyStore.class);
        fakePropertyStringStore = mock(DynamicStringStore.class);
        fakePropertyArrayStore = mock(DynamicArrayStore.class);
        fakeMetaDataStore = mock(MetaDataStore.class);
        doReturn(fakeNodeStore).when(neoStores).getNodeStore();
        doReturn(fakeDynamicLabelStore).when(fakeNodeStore).getDynamicLabelStore();
        doReturn(fakeRelStore).when(neoStores).getRelationshipStore();
        doReturn(fakeLabelTokenStore).when(neoStores).getLabelTokenStore();
        doReturn(fakeLabelTokenNameStore).when(fakeLabelTokenStore).getNameStore();
        doReturn(fakePropKeyStore).when(neoStores).getPropertyKeyTokenStore();
        doReturn(fakePropKeyNameStore).when(fakePropKeyStore).getNameStore();
        doReturn(fakeRelTypeTokenStore).when(neoStores).getRelationshipTypeTokenStore();
        doReturn(fakeRelTypeNameStore).when(fakeRelTypeTokenStore).getNameStore();
        doReturn(fakeRelGroupStore).when(neoStores).getRelationshipGroupStore();
        doReturn(fakeSchemaStore).when(neoStores).getSchemaStore();
        doReturn(fakePropertyStore).when(neoStores).getPropertyStore();
        doReturn(fakePropertyStringStore).when(fakePropertyStore).getStringStore();
        doReturn(fakePropertyArrayStore).when(fakePropertyStore).getArrayStore();
        doReturn(fakeMetaDataStore).when(neoStores).getMetaDataStore();
    }

    @Test
    void shouldAllocateOncePerStore() throws IOException {
        try (PreAllocationTransactionApplier applier = new PreAllocationTransactionApplier(neoStores)) {
            // Nodes
            applier.visitNodeCommand(Commands.createNode(10, 2, 3));
            applier.visitNodeCommand(Commands.createNode(20, 4, 5));

            // Relationships
            applier.visitRelationshipCommand(Commands.createRelationship(4, 10, 20, 0));
            applier.visitRelationshipCommand(Commands.createRelationship(45, 10, 20, 1));

            // Label tokens
            applier.visitLabelTokenCommand(Commands.createLabelToken(3, 3));
            applier.visitLabelTokenCommand(Commands.createLabelToken(5, 4));

            // Property tokens
            applier.visitPropertyKeyTokenCommand(Commands.createPropertyKeyToken(3, 0));
            applier.visitPropertyKeyTokenCommand(Commands.createPropertyKeyToken(7, 1));

            // Relationship type tokens
            applier.visitRelationshipTypeTokenCommand(Commands.createRelationshipTypeToken(2, 0));
            applier.visitRelationshipTypeTokenCommand(Commands.createRelationshipTypeToken(9, 7));

            // Relationship groups
            applier.visitRelationshipGroupCommand(Commands.createRelationshipGroup(11, 1));
            applier.visitRelationshipGroupCommand(Commands.createRelationshipGroup(22, 2));

            // Schema rules
            applier.visitSchemaRuleCommand(
                    Commands.createIndexRule(PROVIDER_DESCRIPTOR, 10, SchemaDescriptors.forLabel(0, 1)));
            applier.visitSchemaRuleCommand(
                    Commands.createIndexRule(PROVIDER_DESCRIPTOR, 5, SchemaDescriptors.forLabel(1, 2)));

            // Properties
            applier.visitPropertyCommand(Commands.createProperty(10, PropertyType.STRING, 0, 1, 2));
            applier.visitPropertyCommand(Commands.createProperty(15, PropertyType.STRING, 2, 3, 4));
            applier.visitPropertyCommand(Commands.createProperty(20, PropertyType.ARRAY, 1, 5, 6));
            applier.visitPropertyCommand(Commands.createProperty(25, PropertyType.ARRAY, 3, 7, 8));
        }

        verify(fakeNodeStore, times(1)).allocate(20);
        verify(fakeNodeStore, times(2)).getDynamicLabelStore();
        verify(fakeDynamicLabelStore, times(1)).allocate(5);
        verify(fakeRelStore, times(1)).allocate(45);
        verify(fakeLabelTokenStore, times(1)).allocate(5);
        verify(fakeLabelTokenStore, times(2)).getNameStore();
        verify(fakeLabelTokenNameStore, times(1)).allocate(4);
        verify(fakePropKeyStore, times(1)).allocate(7);
        verify(fakePropKeyStore, times(2)).getNameStore();
        verify(fakePropKeyNameStore, times(1)).allocate(1);
        verify(fakeRelTypeTokenStore, times(1)).allocate(9);
        verify(fakeRelTypeTokenStore, times(2)).getNameStore();
        verify(fakeRelTypeNameStore, times(1)).allocate(7);
        verify(fakeRelGroupStore, times(1)).allocate(22);
        verify(fakeSchemaStore, times(1)).allocate(10);
        verify(fakePropertyStore, times(1)).allocate(25);
        verify(fakePropertyStore, times(2)).getStringStore();
        verify(fakePropertyStore, times(2)).getArrayStore();
        verify(fakePropertyStringStore, times(1)).allocate(4);
        verify(fakePropertyArrayStore, times(1)).allocate(8);

        verifyNoMoreInteractionsOnStores();
    }

    @Test
    void shouldPreallocateHighestOfSecondaryAndFirst() throws IOException {
        try (PreAllocationTransactionApplier applier = new PreAllocationTransactionApplier(neoStores)) {
            NodeRecord node = new NodeRecord(5).initialize(true, 123, true, 456, 0);
            node.setSecondaryUnitIdOnLoad(6);
            RelationshipRecord relationship =
                    new RelationshipRecord(12).initialize(true, 1, 2, 3, 4, 5, 6, 7, 8, true, true);
            relationship.setSecondaryUnitIdOnLoad(10);
            RelationshipGroupRecord relationshipGroup =
                    new RelationshipGroupRecord(8).initialize(true, 0, 1, 2, 3, 4, 5);
            relationshipGroup.setSecondaryUnitIdOnLoad(20);

            applier.visitNodeCommand(new NodeCommand(LATEST_LOG_SERIALIZATION, new NodeRecord(node.getId()), node));
            applier.visitRelationshipCommand(new RelationshipCommand(
                    LATEST_LOG_SERIALIZATION, new RelationshipRecord(relationship.getId()), relationship));
            applier.visitRelationshipGroupCommand(new Command.RelationshipGroupCommand(
                    LATEST_LOG_SERIALIZATION,
                    new RelationshipGroupRecord(relationshipGroup.getId()),
                    relationshipGroup));
        }

        verify(fakeNodeStore, times(1)).allocate(6);
        verify(fakeNodeStore, times(1)).getDynamicLabelStore();
        verify(fakeRelStore, times(1)).allocate(12);
        verify(fakeRelGroupStore, times(1)).allocate(20);

        verifyNoMoreInteractionsOnStores();
    }

    @Test
    void shouldNotPreallocateForMetadataCommand() throws IOException {
        try (PreAllocationTransactionApplier applier = new PreAllocationTransactionApplier(neoStores)) {
            applier.visitMetaDataCommand(new Command.MetaDataCommand(
                    LATEST_LOG_SERIALIZATION,
                    new MetaDataRecord().initialize(true, 1),
                    new MetaDataRecord().initialize(true, 2)));
        }

        verifyNoMoreInteractionsOnStores();
    }

    private void verifyNoMoreInteractionsOnStores() {
        verifyNoMoreInteractions(
                fakeNodeStore,
                fakeDynamicLabelStore,
                fakeRelStore,
                fakeLabelTokenStore,
                fakeLabelTokenNameStore,
                fakePropKeyStore,
                fakePropKeyNameStore,
                fakeRelTypeTokenStore,
                fakeRelTypeNameStore,
                fakeRelGroupStore,
                fakeSchemaStore,
                fakePropertyStore,
                fakePropertyStringStore,
                fakePropertyArrayStore,
                fakeMetaDataStore);
    }
}
