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

import java.util.Map;
import java.util.Set;

public final class Neo4jDiagnosticRecordProperty {
    public static final NonGqlStandardDiagnosticRecordProperty<String> SEVERITY =
            NonGqlStandardDiagnosticRecordProperty.Builder.<String>fromKey("_severity")
                    .build();

    public static final NonGqlStandardDiagnosticRecordProperty<GqlClassification> CLASSIFICATION =
            NonGqlStandardDiagnosticRecordProperty.Builder.<GqlClassification>fromKey("_classification")
                    .withValueOmittedPredicate(Neo4jDiagnosticRecordProperty::isOmittedClassification)
                    .withValueSerializer(String::valueOf)
                    .build();

    public static final NonGqlStandardDiagnosticRecordProperty<Map<String, Object>> STATUS_PARAMETERS =
            NonGqlStandardDiagnosticRecordProperty.Builder.<Map<String, Object>>fromKey("_status_parameters")
                    // TODO: enable this line again when re-introducing status parameters
                    .disabled()
                    .build();

    public static final NonGqlStandardDiagnosticRecordProperty<Position> POSITION =
            NonGqlStandardDiagnosticRecordProperty.Builder.<Position>fromKey("_position")
                    .withValueSerializer(pos -> ((Position) pos).asMap())
                    .build();

    private static final Set<DiagnosticRecordProperty<?>> ALL_PROPERTIES = Set.of(SEVERITY, CLASSIFICATION, POSITION);

    public static Set<DiagnosticRecordProperty<?>> asSet() {
        return ALL_PROPERTIES;
    }

    private static boolean isOmittedClassification(GqlClassification classification) {
        return classification == ErrorClassification.UNKNOWN || classification == NotificationClassification.UNKNOWN;
    }

    public record Position(int offset, int line, int column) {
        public Map<String, Integer> asMap() {
            return Map.of("offset", offset, "line", line, "column", column);
        }
    }
}
