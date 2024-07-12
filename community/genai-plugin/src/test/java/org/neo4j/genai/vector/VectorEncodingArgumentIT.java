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
package org.neo4j.genai.vector;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.Test;
import org.neo4j.genai.util.GenAIExtension;
import org.neo4j.genai.vector.providers.TestProvider;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@DbmsExtension(configurationCallback = "configure")
class VectorEncodingArgumentIT extends VectorEncodingArgumentBase {

    @Inject
    private GraphDatabaseAPI database;

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.addExtension(new GenAIExtension());
    }

    static Value singleResultItem(Result result) {
        if (result.hasNext()) {
            var vector = result.next().get("vector");
            return vector == null ? Values.NO_VALUE : Values.floatArray((float[]) vector);
        }
        return Values.NO_VALUE;
    }

    @Override
    Value single(String resource, String provider, Map<String, ?> configuration) {
        return database.executeTransactionally(
                "RETURN genai.vector.encode($resource, $provider, $configuration) AS vector",
                Maps.mutable.of("resource", resource, "provider", provider, "configuration", configuration),
                VectorEncodingArgumentIT::singleResultItem);
    }

    @Override
    List<Value> batch(List<String> resources, String provider, Map<String, ?> configuration) {
        return database.executeTransactionally(
                "CALL genai.vector.encodeBatch($resources, $provider, $configuration)",
                Maps.mutable.of("resources", resources, "provider", provider, "configuration", configuration),
                new ResultTransformer<>() {
                    private final List<Value> vectors = new ArrayList<>();

                    @Override
                    public List<Value> apply(Result result) {
                        while (result.hasNext()) {
                            vectors.add(singleResultItem(result));
                        }
                        return vectors;
                    }
                });
    }

    @Test
    void missingConfigIsValidIfNotNeededSingle() {
        assertThatCode(() -> database.executeTransactionally(
                        "RETURN genai.vector.encode($resource, $provider)",
                        Maps.mutable.of("resource", "something", "provider", TestProvider.NAME)))
                .as("missing config")
                .doesNotThrowAnyException();
    }

    @Test
    void missingConfigIsValidIfNotNeededBatched() {
        assertThatCode(() -> database.executeTransactionally(
                        "CALL genai.vector.encodeBatch($resources, $provider)",
                        Maps.mutable.of("resources", List.of("something", "other"), "provider", TestProvider.NAME)))
                .as("missing config")
                .doesNotThrowAnyException();
    }
}
