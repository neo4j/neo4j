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
package org.neo4j.kernel.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.diagnostics.DiagnosticsReportManifest.ClassifierResult;
import org.neo4j.kernel.diagnostics.DiagnosticsReportManifest.SourceResult;
import org.neo4j.kernel.internal.Version;

class DiagnosticsReportManifestTest {
    private static final OffsetDateTime TIMESTAMP =
            OffsetDateTime.of(2026, 7, 23, 14, 15, 30, 123_000_000, ZoneOffset.ofHours(2));
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void classifierStatusReflectsSourceOutcomes() {
        assertThat(classifier(source("a", true), source("b", true)).status()).isEqualTo(CollectionStatus.SUCCESS);
        assertThat(classifier(source("a", true), source("b", false)).status()).isEqualTo(CollectionStatus.PARTIAL);
        assertThat(classifier(source("a", false), source("b", false)).status()).isEqualTo(CollectionStatus.FAILED);
    }

    @Test
    void jsonContainsVersionAndSchemaVersion() throws Exception {
        JsonNode root = MAPPER.readTree(manifest(List.of()).toJson());

        assertThat(root.get("schemaVersion").asText()).isEqualTo("1.0");
        assertThat(root.get("neo4jVersion").asText()).isEqualTo(Version.getNeo4jVersion());
        // textValue() rather than asText(), since the jar manifest version is null outside of a packaged jar.
        assertThat(root.get("manifestVersion").textValue()).isEqualTo(Version.getManifestVersion());
        assertThat(root.get("classifiers").isArray()).isTrue();
        assertThat(root.get("classifiers")).isEmpty();
    }

    @Test
    void jsonContainsHostnameAndZonedSecondPrecisionTimestamp() throws Exception {
        JsonNode root = MAPPER.readTree(manifest(List.of()).toJson());
        assertThat(root.get("hostname").asText()).isEqualTo("my-host");
        // truncated to seconds, with the host's zone offset
        assertThat(root.get("timestamp").asText()).isEqualTo("2026-07-23T14:15:30+02:00");

        JsonNode utc = MAPPER.readTree(
                new DiagnosticsReportManifest("my-host", TIMESTAMP.withOffsetSameLocal(ZoneOffset.UTC), List.of())
                        .toJson());
        assertThat(utc.get("timestamp").asText()).isEqualTo("2026-07-23T14:15:30Z");
    }

    @Test
    void jsonReportsPerSourceOutcomesGroupedByClassifier() throws Exception {
        String json = manifest(List.of(
                        new ClassifierResult("config", List.of(source("config/neo4j.conf", true))),
                        new ClassifierResult(
                                "logs", List.of(source("logs/debug.log", true), source("logs/gc.log", false, "boom")))))
                .toJson();

        // Records serialize their components only - the succeeded() helper must not leak as a field.
        assertThat(json).doesNotContain("succeeded");

        JsonNode classifiers = MAPPER.readTree(json).get("classifiers");
        JsonNode config = classifier(classifiers, "config");
        assertThat(config.get("status").asText()).isEqualTo("SUCCESS");
        assertThat(config.get("sources").get(0).get("path").asText()).isEqualTo("config/neo4j.conf");
        assertThat(config.get("sources").get(0).get("status").asText()).isEqualTo("SUCCESS");

        JsonNode logs = classifier(classifiers, "logs");
        assertThat(logs.get("status").asText()).isEqualTo("PARTIAL");
        // Sources are ordered by path: debug.log (SUCCESS) then gc.log (FAILED).
        assertThat(logs.get("sources").get(0).get("path").asText()).isEqualTo("logs/debug.log");
        assertThat(logs.get("sources").get(0).get("status").asText()).isEqualTo("SUCCESS");
        assertThat(logs.get("sources").get(1).get("path").asText()).isEqualTo("logs/gc.log");
        assertThat(logs.get("sources").get(1).get("status").asText()).isEqualTo("FAILED");
        assertThat(logs.get("sources").get(1).get("error").asText()).isEqualTo("boom");
    }

    @Test
    void jsonEscapesSpecialCharactersInErrorMessages() throws Exception {
        String json = manifest(List.of(new ClassifierResult(
                        "logs", List.of(source("logs/debug.log", false, "line1\n\"quoted\"\tend")))))
                .toJson();

        JsonNode source =
                MAPPER.readTree(json).get("classifiers").get(0).get("sources").get(0);
        assertThat(source.get("error").asText()).isEqualTo("line1\n\"quoted\"\tend");
    }

    private static JsonNode classifier(JsonNode classifiers, String name) {
        for (JsonNode classifier : classifiers) {
            if (name.equals(classifier.get("name").asText())) {
                return classifier;
            }
        }
        throw new AssertionError("No classifier named '" + name + "' in " + classifiers);
    }

    private static DiagnosticsReportManifest manifest(List<ClassifierResult> classifiers) {
        return new DiagnosticsReportManifest("my-host", TIMESTAMP, classifiers);
    }

    private static ClassifierResult classifier(SourceResult... sources) {
        return new ClassifierResult("classifier", List.of(sources));
    }

    private static SourceResult source(String path, boolean succeeded) {
        return source(path, succeeded, succeeded ? null : "failure");
    }

    private static SourceResult source(String path, boolean succeeded, String error) {
        return new SourceResult(path, succeeded ? CollectionStatus.SUCCESS : CollectionStatus.FAILED, error);
    }
}
