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
package org.neo4j.gqlstatus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Entries/fields in this "map" must be serializable* (Bolt must be able to parse them).
 * They must either be primitive types like int, or String
 * or collections that consists of such types (like _position)
 */
public class DiagnosticRecord {
    private static final String CURRENT_SCHEMA_DEFAULT = "/";
    private static final String OPERATION_DEFAULT = "";
    private static final String OPERATION_CODE_DEFAULT = "0";
    private final Map<String, Object> innerDiagnosticRecord;

    public DiagnosticRecord(
            String severity,
            GqlClassification classification,
            int offset,
            int line,
            int column,
            Map<String, Object> statusParameters) {
        innerDiagnosticRecord = new HashMap<>();
        innerDiagnosticRecord.put("CURRENT_SCHEMA", CURRENT_SCHEMA_DEFAULT);
        innerDiagnosticRecord.put("OPERATION", OPERATION_DEFAULT);
        innerDiagnosticRecord.put("OPERATION_CODE", OPERATION_CODE_DEFAULT);
        innerDiagnosticRecord.put("_severity", severity);
        innerDiagnosticRecord.put("_classification", String.valueOf(classification));
        innerDiagnosticRecord.put("_position", Map.of("offset", offset, "line", line, "column", column));
        innerDiagnosticRecord.put("_status_parameters", statusParameters);
    }

    public DiagnosticRecord(String severity, GqlClassification classification, int offset, int line, int column) {
        innerDiagnosticRecord = new HashMap<>();
        innerDiagnosticRecord.put("CURRENT_SCHEMA", CURRENT_SCHEMA_DEFAULT);
        innerDiagnosticRecord.put("OPERATION", OPERATION_DEFAULT);
        innerDiagnosticRecord.put("OPERATION_CODE", OPERATION_CODE_DEFAULT);
        innerDiagnosticRecord.put("_severity", severity);
        innerDiagnosticRecord.put("_classification", String.valueOf(classification));
        innerDiagnosticRecord.put("_position", Map.of("offset", offset, "line", line, "column", column));
    }

    public DiagnosticRecord() {
        innerDiagnosticRecord = new HashMap<>();
        innerDiagnosticRecord.put("CURRENT_SCHEMA", CURRENT_SCHEMA_DEFAULT);
        innerDiagnosticRecord.put("OPERATION", OPERATION_DEFAULT);
        innerDiagnosticRecord.put("OPERATION_CODE", OPERATION_CODE_DEFAULT);
    }

    private DiagnosticRecord(Map<String, Object> jsonMap) {
        innerDiagnosticRecord = new HashMap<>();
        var CURRENT_SCHEMA = Optional.ofNullable(jsonMap.get("CURRENT_SCHEMA"));
        var OPERATION = Optional.ofNullable(jsonMap.get("OPERATION"));
        var OPERATION_CODE = Optional.ofNullable(jsonMap.get("OPERATION_CODE"));
        var _severity = Optional.ofNullable(jsonMap.get("_severity"));
        var _classification = Optional.ofNullable(jsonMap.get("_classification"));
        var _position = Optional.ofNullable(jsonMap.get("_position"));
        var _status_parameters = Optional.ofNullable(jsonMap.get("_status_parameters"));

        // These are always put in, default value if value is missing
        innerDiagnosticRecord.put("CURRENT_SCHEMA", CURRENT_SCHEMA.orElse(CURRENT_SCHEMA_DEFAULT));
        innerDiagnosticRecord.put("OPERATION", OPERATION.orElse(OPERATION_DEFAULT));
        innerDiagnosticRecord.put("OPERATION_CODE", OPERATION_CODE.orElse(OPERATION_CODE_DEFAULT));

        // These might have been put in, but doesn't have a default value
        _severity.ifPresent((s) -> innerDiagnosticRecord.put("_severity", s));
        _classification.ifPresent((c) -> innerDiagnosticRecord.put("_classification", c));
        _position.ifPresent((p) -> innerDiagnosticRecord.put("_position", p));
        _status_parameters.ifPresent((sp) -> innerDiagnosticRecord.put("_status_parameters", sp));
    }

    public void setStatusParameters(Map<String, Object> statusParameters) {
        innerDiagnosticRecord.put("_status_parameters", statusParameters);
    }

    public int hashCode() {
        return innerDiagnosticRecord.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DiagnosticRecord that = (DiagnosticRecord) o;
        return innerDiagnosticRecord.equals(that.innerDiagnosticRecord);
    }

    public Map<String, Object> asMap() {
        return innerDiagnosticRecord;
    }

    // This is not used right now, but will be later when Gql is included in logs
    // This returns an Optional since this operation might fail and throw a JsonProcessingException
    // JsonProcessingException is part of the Jackson library, and to avoid other modules depending
    // on that library, we catch it here and return an Optional instead
    // We could also catch the JsonProcessingException and return a new error instead,
    // But right now we go for Optional
    public Optional<String> asJson() {
        var mapper = new ObjectMapper();
        try {
            var json = mapper.writeValueAsString(this.innerDiagnosticRecord);
            return Optional.of(json);
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    // This is not used right now, but could be useful
    public static Optional<DiagnosticRecord> fromJson(String json) {
        var mapper = new ObjectMapper();
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = mapper.readValue(json, Map.class);
            return Optional.of(new DiagnosticRecord(parsed));
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }
}
