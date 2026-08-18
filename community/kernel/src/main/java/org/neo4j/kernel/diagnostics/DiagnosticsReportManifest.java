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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.neo4j.kernel.internal.Version;

public class DiagnosticsReportManifest {
    /**
     * Schema version of the manifest itself. Bump this when the structure changes in a backwards-incompatible way.
     */
    public static final String SCHEMA_VERSION = "1.0";

    public static final String FILE_NAME = "manifest.json";

    /**
     * The outcome of writing a single source into the archive. This is the serialized shape: its components map
     * directly onto the JSON members {@code path}, {@code status} and {@code error}.
     *
     * @param path the path of the source within the archive.
     * @param status {@link CollectionStatus#SUCCESS} if the source was written, otherwise {@link CollectionStatus#FAILED}.
     * @param error a description of why the source failed, or {@code null} when it succeeded.
     */
    public record SourceResult(String path, CollectionStatus status, String error) {
        public boolean succeeded() {
            return status == CollectionStatus.SUCCESS;
        }
    }

    /**
     * The collected sources for a single classifier and their aggregate outcome. This is the serialized shape: it maps
     * onto the JSON members {@code name}, {@code status} and {@code sources}. The status is derived rather than stored,
     * so that sources can be appended to a result as they are collected.
     */
    @JsonPropertyOrder({"name", "status", "sources"})
    public record ClassifierResult(String name, List<SourceResult> sources) {
        @JsonProperty
        public CollectionStatus status() {
            boolean anySucceeded = false;
            boolean anyFailed = false;
            for (SourceResult source : sources) {
                anySucceeded |= source.succeeded();
                anyFailed |= !source.succeeded();
            }
            if (!anyFailed) {
                return CollectionStatus.SUCCESS;
            }
            return anySucceeded ? CollectionStatus.PARTIAL : CollectionStatus.FAILED;
        }
    }

    private static final ObjectWriter JSON = new ObjectMapper().writerWithDefaultPrettyPrinter();

    public final String schemaVersion;
    public final String neo4jVersion;
    public final String manifestVersion;
    public final String hostname;
    public final String timestamp;
    public final List<ClassifierResult> classifiers;

    public DiagnosticsReportManifest(String hostname, OffsetDateTime timestamp, List<ClassifierResult> classifiers) {
        this.schemaVersion = SCHEMA_VERSION;
        this.neo4jVersion = Version.getNeo4jVersion();
        this.manifestVersion = Version.getManifestVersion();
        this.hostname = hostname;
        // Second precision with the host's zone offset, so it lines up with the second-precision file name timestamp.
        this.timestamp = timestamp.truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        this.classifiers = classifiers;
    }

    public String toJson() {
        try {
            return JSON.writeValueAsString(this);
        } catch (IOException e) { // JsonProcessingException, thrown by writeValueAsString, is an IOException
            throw new UncheckedIOException("Failed to serialize diagnostics report manifest", e);
        }
    }
}
