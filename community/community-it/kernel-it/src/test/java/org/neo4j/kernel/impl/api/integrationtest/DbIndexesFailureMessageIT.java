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
package org.neo4j.kernel.impl.api.integrationtest;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.FailingNativeIndexProviderFactory.FailureType.POPULATION;

import java.nio.file.Path;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.neo4j.cypher.internal.javacompat.ResultRowImpl;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.FailingNativeIndexProviderFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class DbIndexesFailureMessageIT extends KernelIntegrationTest {
    @Test
    void listAllIndexesWithFailedIndex() throws Throwable {
        // Given
        KernelTransaction dataTransaction = newTransaction(AUTH_DISABLED);
        String labelName = "Fail";
        String propertyKey = "foo";
        int failedLabel = dataTransaction.tokenWrite().labelGetOrCreateForName(labelName);
        int propertyKeyId1 = dataTransaction.tokenWrite().propertyKeyGetOrCreateForName(propertyKey);
        this.transaction.createNode(Label.label(labelName)).setProperty(propertyKey, "some value");
        commit();

        KernelTransaction transaction = newTransaction(AUTH_DISABLED);
        LabelSchemaDescriptor schema = forLabel(failedLabel, propertyKeyId1);
        String indexName = "fail foo index";
        IndexDescriptor index = transaction
                .schemaWrite()
                .indexCreate(IndexPrototype.forSchema(schema, FailingNativeIndexProviderFactory.DESCRIPTOR)
                        .withName(indexName));
        long indexId = index.getId();
        String indexProvider = index.getIndexProvider().name();
        Map<String, Value> indexConfig = index.getIndexConfig().asMap();
        commit();

        try (Transaction tx = db.beginTx()) {
            assertThatThrownBy(() -> tx.schema().awaitIndexesOnline(2, MINUTES))
                    .isInstanceOf(IllegalStateException.class);
        }

        // When
        try (Transaction tx = db.beginTx()) {
            Result results = tx.execute("SHOW INDEX YIELD * WHERE name = '" + index.getName() + "'");
            assertThat(results).hasNext();
            Result.ResultRow result = new ResultRowImpl(results.next());
            assertThat(results).isExhausted();

            assertThat(result.getNumber("id")).as("id").isEqualTo(indexId);
            assertThat(result.getString("name")).as("name").isEqualTo(indexName);
            assertThat(result.getString("state")).as("state").isEqualTo("FAILED");
            assertThat(result.getNumber("populationPercent"))
                    .as("populationPercent")
                    .isEqualTo(0.0);
            assertThat(result.getString("owningConstraint"))
                    .as("owningConstraint")
                    .isEqualTo(null);
            assertThat(result.getString("type")).as("type").isEqualTo("RANGE");
            assertThat(result.getString("entityType")).as("entityType").isEqualTo("NODE");
            assertThat(result.get("labelsOrTypes"))
                    .asInstanceOf(LIST)
                    .as("labelsOrTypes")
                    .containsExactly(labelName);
            assertThat(result.get("properties"))
                    .asInstanceOf(LIST)
                    .as("properties")
                    .containsExactly(propertyKey);
            assertThat(result.getString("indexProvider")).as("indexProvider").isEqualTo(indexProvider);
            assertThat(result.get("options"))
                    .extracting("indexConfig", InstanceOfAssertFactories.map(String.class, Object.class))
                    .extractingFromEntries(entry -> Map.entry(entry.getKey(), Values.of(entry.getValue())))
                    .as("indexConfig")
                    .containsExactlyInAnyOrderElementsOf(indexConfig.entrySet());
            assertThat(result.getString("failureMessage"))
                    .as("failureMessage")
                    .startsWith("java.lang.RuntimeException: Fail on update during population");
            assertThat(result.getString("createStatement"))
                    .as("createStatement")
                    .contains("CREATE RANGE INDEX", indexName, "FOR", labelName, "ON", propertyKey);
        }
    }

    @Test
    void indexDetailsWithNonExistingIndex() {
        try (Transaction tx = db.beginTx()) {
            Result results = tx.execute("SHOW INDEX YIELD * WHERE name = 'MyIndex'");
            assertThat(results).isExhausted();
        }
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory(Path databaseRootDir) {
        return super.createGraphDatabaseFactory(databaseRootDir)
                .noOpSystemGraphInitializer()
                .addExtension(new FailingNativeIndexProviderFactory(POPULATION));
    }
}
