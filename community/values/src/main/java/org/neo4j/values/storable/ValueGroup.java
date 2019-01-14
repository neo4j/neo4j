/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import static org.neo4j.values.storable.ValueCategory.NO_CATEGORY;
import static org.neo4j.values.storable.ValueCategory.REST;
import static org.neo4j.values.storable.ValueCategory.TEMPORAL;

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
public enum ValueGroup
{
    UNKNOWN(                ValueCategory.UNKNOWN ),
    GEOMETRY_ARRAY(         REST ),
    ZONED_DATE_TIME_ARRAY(  REST ),
    LOCAL_DATE_TIME_ARRAY(  REST ),
    DATE_ARRAY(             REST ),
    ZONED_TIME_ARRAY(       REST ),
    LOCAL_TIME_ARRAY(       REST ),
    DURATION_ARRAY(         REST ),
    TEXT_ARRAY(             REST ),
    BOOLEAN_ARRAY(          REST ),
    NUMBER_ARRAY(           REST ),
    GEOMETRY(               ValueCategory.GEOMETRY ),
    ZONED_DATE_TIME(        TEMPORAL ),
    LOCAL_DATE_TIME(        TEMPORAL ),
    DATE(                   TEMPORAL ),
    ZONED_TIME(             TEMPORAL ),
    LOCAL_TIME(             TEMPORAL ),
    DURATION(               TEMPORAL ),
    TEXT(                   ValueCategory.TEXT ),
    BOOLEAN(                REST ),
    NUMBER(                 ValueCategory.NUMBER ),
    NO_VALUE(               NO_CATEGORY );

    private final ValueCategory category;

    ValueGroup( ValueCategory category )
    {
        this.category = category;
    }

    public ValueCategory category()
    {
        return category;
    }
}
