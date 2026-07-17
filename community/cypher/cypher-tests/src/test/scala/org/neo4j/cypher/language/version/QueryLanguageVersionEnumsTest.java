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
package org.neo4j.cypher.language.version;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.CypherVersionClassification;
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage$;
import org.neo4j.cypher.internal.options.CypherVersionOption;
import scala.jdk.javaapi.OptionConverters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static scala.collection.JavaConverters.setAsJavaSet;

class QueryLanguageVersionEnumsTest {

    // We have too many of these enums, this test is here to make sure they stay in sync.
    @Test
    void queryLanguageEnumsMatchUp() {
        final var expectedShortNames = List.of("5", "25");

        assertThat(org.neo4j.cypher.internal.CypherVersion.values())
                .as("org.neo4j.cypher.internal.CypherVersion contains expected version names")
                .extracting(v -> v.versionName)
                .containsExactlyElementsOf(expectedShortNames);

        assertThat(GraphDatabaseSettings.CypherVersion.values())
                .as("GraphDatabaseSettings.CypherVersion contains expected values")
                .extracting(v -> v.toString().replace("CYPHER_", ""))
                .containsExactlyElementsOf(expectedShortNames);

        assertThat(GraphDatabaseSettings.CypherVersion.values())
                .as("GraphDatabaseSettings.CypherVersion contains expected short names")
                .extracting(CypherVersionClassification::shortName)
                .containsExactlyElementsOf(expectedShortNames);

        assertThat(org.neo4j.cypher.internal.CypherVersion.values())
                .as("org.neo4j.cypher.internal.CypherVersion.experimental is in sync with GraphDatabaseSettings.CypherVersion")
                .allSatisfy(v -> {
           final var expected = Arrays.stream(GraphDatabaseSettings.CypherVersion.values())
                   .filter(sv -> CypherVersionClassification.shortName(sv).equals(v.versionName))
                   .collect(Collectors.toSet());
           assertThat(expected).hasSize(1);
           assertThat(v.experimental).isEqualTo(CypherVersionClassification.isExperimental(expected.iterator().next()));
        });

        assertThat(org.neo4j.kernel.api.QueryLanguage.values())
                .as("org.neo4j.kernel.api.QueryLanguage contains expected values")
                .extracting(v -> v.name().replace("CYPHER_", ""))
                .containsExactlyInAnyOrderElementsOf(expectedShortNames);

        assertThat(setAsJavaSet(org.neo4j.cypher.internal.frontend.phases.QueryLanguage$.MODULE$.All()))
                .as("org.neo4j.cypher.internal.frontend.phases.QueryLanguage maps to org.neo4j.cypher.internal.CypherVersion")
                .extracting(QueryLanguage$.MODULE$::toCypherVersion)
                .containsExactlyInAnyOrder(org.neo4j.cypher.internal.CypherVersion.values());

        assertThat(setAsJavaSet(org.neo4j.cypher.internal.frontend.phases.QueryLanguage$.MODULE$.All()))
                .as("org.neo4j.cypher.internal.frontend.phases.QueryLanguage maps to org.neo4j.kernel.api.QueryLanguage")
                .extracting(QueryLanguage$.MODULE$::toKernelScope)
                .containsExactlyInAnyOrder(org.neo4j.kernel.api.QueryLanguage.values());

        assertThat(setAsJavaSet(CypherVersionOption.values()))
                .as("CypherVersionOption contains expected values")
                .extracting(CypherVersionOption::version)
                .containsExactlyInAnyOrderElementsOf(expectedShortNames);

        assertThat(setAsJavaSet(CypherVersionOption.values()))
                .as("CypherVersionOption maps to org.neo4j.cypher.internal.CypherVersion")
                .extracting(v ->  OptionConverters.toJava(v.explicitVersion()).orElse(null))
                .containsExactlyInAnyOrder(org.neo4j.cypher.internal.CypherVersion.values());
    }

    @Test
    void rememberToFollowTheProcess() {
        assertThat(GraphDatabaseSettings.CypherVersion.values())
                .as(
            """
                      Hello!

                      You have introduced a new language version, congratulations!
                      There's a lot of work ahead of you.
                      To make your life easier we have compiled a checklist of things you (probably) need to remember.
                      Please go to https://trello.com/c/a3IRl1Zu and follow the instructions there.
                      Good luck!
            """)
                .containsExactly(GraphDatabaseSettings.CypherVersion.Cypher5, GraphDatabaseSettings.CypherVersion.Cypher25);
    }
}
