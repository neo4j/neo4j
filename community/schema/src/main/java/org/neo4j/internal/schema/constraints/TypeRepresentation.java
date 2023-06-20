/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.schema.constraints;

public sealed interface TypeRepresentation permits SchemaValueType {
    String userDescription();

    Ordering order();

    enum Ordering {
        BOOLEAN_ORDER,
        STRING_ORDER,
        INTEGER_ORDER,
        FLOAT_ORDER,
        DATE_ORDER,
        LOCAL_TIME_ORDER,
        ZONED_TIME_ORDER,
        LOCAL_DATETIME_ORDER,
        ZONED_DATETIME_ORDER,
        DURATION_ORDER,
        POINT_ORDER;
    }

    static int compare(TypeRepresentation t1, TypeRepresentation t2) {
        return t1.order().compareTo(t2.order());
    }
}
