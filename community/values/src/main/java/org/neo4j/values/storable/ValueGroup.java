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
package org.neo4j.values.storable;

/**
 * The ValueGroup is the logical group or type of a Value. For example byte, short, int and long are all attempting
 * to represent mathematical integers, meaning that for comparison purposes they should be treated the same.
 *
 * The order here is defined in <a href="https://github.com/opencypher/openCypher/blob/master/cip/1.accepted/CIP2016-06-14-Define-comparability-and-equality-as-well-as-orderability-and-equivalence.adoc">
 *   The Cypher CIP defining orderability
 * </a>
 *
 * Each ValueGroup belong to some larger grouping called {@link ValueCategory}.
 */
public enum ValueGroup {
    UNKNOWN(ValueCategory.UNKNOWN),
    ANYTHING(ValueCategory.ANYTHING),
    GEOMETRY_ARRAY(ValueCategory.GEOMETRY_ARRAY),
    ZONED_DATE_TIME_ARRAY(ValueCategory.TEMPORAL_ARRAY),
    LOCAL_DATE_TIME_ARRAY(ValueCategory.TEMPORAL_ARRAY),
    DATE_ARRAY(ValueCategory.TEMPORAL_ARRAY),
    ZONED_TIME_ARRAY(ValueCategory.TEMPORAL_ARRAY),
    LOCAL_TIME_ARRAY(ValueCategory.TEMPORAL_ARRAY),
    DURATION_ARRAY(ValueCategory.TEMPORAL_ARRAY),
    TEXT_ARRAY(ValueCategory.TEXT_ARRAY),
    BOOLEAN_ARRAY(ValueCategory.BOOLEAN_ARRAY),
    NUMBER_ARRAY(ValueCategory.NUMBER_ARRAY),
    GEOMETRY(ValueCategory.GEOMETRY),
    ZONED_DATE_TIME(ValueCategory.TEMPORAL),
    LOCAL_DATE_TIME(ValueCategory.TEMPORAL),
    DATE(ValueCategory.TEMPORAL),
    ZONED_TIME(ValueCategory.TEMPORAL),
    LOCAL_TIME(ValueCategory.TEMPORAL),
    DURATION(ValueCategory.TEMPORAL),
    TEXT(ValueCategory.TEXT),
    BOOLEAN(ValueCategory.BOOLEAN),
    NUMBER(ValueCategory.NUMBER),
    NO_VALUE(ValueCategory.NO_CATEGORY);

    private final ValueCategory category;

    ValueGroup(ValueCategory category) {
        this.category = category;
    }

    public ValueCategory category() {
        return category;
    }
}
