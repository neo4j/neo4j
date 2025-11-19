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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Defines all the Gql Standard properties on the Diagnostic Record.
 */
enum GqlStandardDiagnosticRecordProperty implements DiagnosticRecordProperty<String> {
    CURRENT_SCHEMA("CURRENT_SCHEMA", "/"),
    OPERATION_CODE("OPERATION_CODE", "0"),
    OPERATION("OPERATION", ""),
    ;
    private static final Set<DiagnosticRecordProperty<?>> ALL_PROPERTIES =
            Collections.unmodifiableSet(EnumSet.allOf(GqlStandardDiagnosticRecordProperty.class));

    private final String defaultValue;
    private final String key;

    GqlStandardDiagnosticRecordProperty(String key, String defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    static Map<String, Object> asMap() {
        var map = new HashMap<String, Object>();
        for (var property : ALL_PROPERTIES) {
            if (property.disabled()) {
                continue;
            }
            property.defaultValue().ifPresent(value -> map.put(property.key(), value));
        }
        return map;
    }

    static Set<DiagnosticRecordProperty<?>> asSet() {
        return ALL_PROPERTIES;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public Optional<String> defaultValue() {
        return Optional.ofNullable(defaultValue);
    }
}
