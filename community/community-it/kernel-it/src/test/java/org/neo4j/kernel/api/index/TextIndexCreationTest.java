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
package org.neo4j.kernel.api.index;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.schema.IndexType.TEXT;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipOnSpd;

@SkipOnSpd(reason = "Unsupported in SPD")
@ImpermanentDbmsExtension(configurationCallback = "configuration")
public class TextIndexCreationTest {
    @Inject
    private GraphDatabaseAPI db;

    private int labelId;
    private int relTypeId;
    private int[] propertyIds;
    private int[] compositeKey;

    /**
     * To be overridden by subclass
     */
    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseInternalSettings.always_use_latest_index_provider, false);
    }

    /**
     * To be overridden ny subclass
     */
    protected IndexProviderDescriptor getIndexProviderDescriptor() {
        return AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR;
    }

    @BeforeEach
    void setup() throws Exception {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenWrite tokenWrite = ktx.tokenWrite();
            labelId = tokenWrite.labelGetOrCreateForName(label("PERSON").name());
            relTypeId = tokenWrite.relationshipTypeGetOrCreateForName(
                    RelationshipType.withName("FRIEND").name());
            propertyIds = new int[] {tokenWrite.propertyKeyGetOrCreateForName("name")};
            compositeKey = new int[] {
                tokenWrite.propertyKeyGetOrCreateForName("address"), tokenWrite.propertyKeyGetOrCreateForName("age")
            };
            tx.commit();
        }
    }

    @Test
    void shouldRejectCompositeKeys() {
        assertUnsupported(() -> createTextIndex("nti", SchemaDescriptors.forLabel(labelId, compositeKey)));
        assertUnsupported(() -> createTextIndex("rti", SchemaDescriptors.forRelType(relTypeId, compositeKey)));
    }

    private void assertUnsupported(ThrowingCallable operation) {
        ErrorGqlStatusObjectAssertions.assertThatThrownBy(operation)
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContainingAll("A composite", TEXT.name(), "index is not supported")
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_51N31);
    }

    @Test
    void shouldCreateIndexes() throws Exception {
        // When
        createTextIndex("node_text_index", SchemaDescriptors.forLabel(labelId, propertyIds));
        createTextIndex("rel_text_index", SchemaDescriptors.forRelType(relTypeId, propertyIds));

        // Then
        awaitIndexesOnline();
        try (Transaction transaction = db.beginTx()) {
            KernelTransaction ktx = ((TransactionImpl) transaction).kernelTransaction();
            assertValidTextIndex(ktx.schemaRead().indexGetForName("node_text_index"), propertyIds);
            assertValidTextIndex(ktx.schemaRead().indexGetForName("rel_text_index"), propertyIds);
        }
    }

    private void awaitIndexesOnline() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, MINUTES);
        }
    }

    private void assertValidTextIndex(IndexDescriptor index, int[] propertyIds) {
        assertThat(index.getIndexType()).isEqualTo(IndexType.TEXT);
        assertThat(index.getCapability().behaviours()).isEmpty();
        assertThat(index.schema().getPropertyIds()).isEqualTo(propertyIds);
        assertThat(index.getCapability().supportsOrdering()).isFalse();
        assertThat(index.getCapability().supportsReturningValues()).isFalse();
    }

    private void createTextIndex(String name, SchemaDescriptor schema) throws Exception {
        try (Transaction tx = db.beginTx()) {
            IndexPrototype prototype = IndexPrototype.forSchema(schema)
                    .withIndexType(IndexType.TEXT)
                    .withIndexProvider(getIndexProviderDescriptor())
                    .withName(name);
            KernelTransaction kernelTransaction = ((InternalTransaction) tx).kernelTransaction();
            kernelTransaction.schemaWrite().indexCreate(prototype);
            tx.commit();
        }
    }
}
