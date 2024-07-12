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

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.genai.util.GenAIExtension;
import org.neo4j.genai.vector.providers.TestProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class VectorEncodingIT {
    @Inject
    private GraphDatabaseAPI database;

    @ExtensionCallback
    public void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.addExtension(new GenAIExtension());
    }

    @Test
    void listProvidersListsLoadedProviders() {
        final var NAME_COLUMN = "name";
        final var REQUIRED_CONFIG_COLUMN = "requiredConfigType";
        final var OPTIONAL_CONFIG_COLUMN = "optionalConfigType";
        final var DEFAULT_CONFIG_COLUMN = "defaultConfig";

        try (var tx = database.beginTx()) {
            assertThat(tx.execute("CALL genai.vector.listEncodingProviders()").stream()
                            .toList())
                    .isSortedAccordingTo(
                            Comparator.comparing(row -> (String) row.get(NAME_COLUMN), CASE_INSENSITIVE_ORDER))
                    .allSatisfy(row -> {
                        assertThat(row)
                                .containsOnlyKeys(
                                        NAME_COLUMN,
                                        REQUIRED_CONFIG_COLUMN,
                                        OPTIONAL_CONFIG_COLUMN,
                                        DEFAULT_CONFIG_COLUMN);

                        assertThat(row.get(REQUIRED_CONFIG_COLUMN)).isInstanceOf(String.class);
                        assertThat(row.get(OPTIONAL_CONFIG_COLUMN)).isInstanceOf(String.class);
                        assertThat(row.get(DEFAULT_CONFIG_COLUMN)).isInstanceOf(Map.class);
                    })
                    .satisfiesOnlyOnce(row -> {
                        assertThat(row.get(NAME_COLUMN)).isEqualTo(TestProvider.NAME);
                        assertThat(row.get(REQUIRED_CONFIG_COLUMN)).isEqualTo(TestProvider.REQUIRED_CONFIG_TYPE);
                        assertThat(row.get(OPTIONAL_CONFIG_COLUMN)).isEqualTo(TestProvider.OPTIONAL_CONFIG_TYPE);
                        assertThat(row.get(DEFAULT_CONFIG_COLUMN)).isEqualTo(TestProvider.DEFAULT_CONFIG);
                    });
        }
    }
}
