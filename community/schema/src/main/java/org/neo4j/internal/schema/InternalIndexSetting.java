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
package org.neo4j.internal.schema;

import org.neo4j.graphdb.schema.IndexSetting;

/// A temporary place to host in-progress {@link IndexSetting}s before moving to
/// the more public {@link org.neo4j.graphdb.schema.IndexSettingImpl}
public enum InternalIndexSetting implements IndexSetting {
    ;

    private final String settingName;
    private final Class<?> valueType;

    InternalIndexSetting(String settingName, Class<?> valueType) {
        this.settingName = settingName;
        this.valueType = valueType;
    }

    @Override
    public String getSettingName() {
        return settingName;
    }

    @Override
    public Class<?> getType() {
        return valueType;
    }
}
