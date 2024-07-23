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
package org.neo4j.graphdb.schema;

public enum IndexSettingImpl implements IndexSetting {
    FULLTEXT_ANALYZER("fulltext.analyzer", String.class),
    FULLTEXT_EVENTUALLY_CONSISTENT("fulltext.eventually_consistent", Boolean.class),
    SPATIAL_CARTESIAN_MIN("spatial.cartesian.min", double[].class),
    SPATIAL_CARTESIAN_MAX("spatial.cartesian.max", double[].class),
    SPATIAL_CARTESIAN_3D_MIN("spatial.cartesian-3d.min", double[].class),
    SPATIAL_CARTESIAN_3D_MAX("spatial.cartesian-3d.max", double[].class),
    SPATIAL_WGS84_MIN("spatial.wgs-84.min", double[].class),
    SPATIAL_WGS84_MAX("spatial.wgs-84.max", double[].class),
    SPATIAL_WGS84_3D_MIN("spatial.wgs-84-3d.min", double[].class),
    SPATIAL_WGS84_3D_MAX("spatial.wgs-84-3d.max", double[].class),
    VECTOR_DIMENSIONS("vector.dimensions", Integer.class),
    VECTOR_SIMILARITY_FUNCTION("vector.similarity_function", String.class),
    VECTOR_QUANTIZATION("vector.quantization", String.class),
    VECTOR_HNSW_M("vector.hnsw.m", Integer.class),
    VECTOR_HNSW_EF_CONSTRUCTION("vector.hnsw.ef_construction", Integer.class),
    ;

    private final String settingName;
    private final Class<?> valueType;

    IndexSettingImpl(String settingName, Class<?> valueType) {
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
