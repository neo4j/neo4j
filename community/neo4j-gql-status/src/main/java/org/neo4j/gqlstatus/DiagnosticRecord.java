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

import java.util.HashMap;
import java.util.Map;

public class DiagnosticRecord {
    private static final String CURRENT_SCHEMA_DEFAULT = "/";
    private static final String OPERATION_DEFAULT = "";
    private static final int OPERATION_CODE_DEFAULT = 0;

    private final Map<String, Object> innerDiagnosticRecord;

    public DiagnosticRecord(
            String severity,
            String classification,
            int offset,
            int line,
            int column,
            Map<String, Object> statusParameters) {
        innerDiagnosticRecord = new HashMap<>();
        innerDiagnosticRecord.put("CURRENT_SCHEMA", CURRENT_SCHEMA_DEFAULT);
        innerDiagnosticRecord.put("OPERATION", OPERATION_DEFAULT);
        innerDiagnosticRecord.put("OPERATION_CODE", OPERATION_CODE_DEFAULT);
        innerDiagnosticRecord.put("_severity", severity);
        innerDiagnosticRecord.put("_classification", classification);
        innerDiagnosticRecord.put("_position", Map.of("offset", offset, "line", line, "column", column));
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
}
