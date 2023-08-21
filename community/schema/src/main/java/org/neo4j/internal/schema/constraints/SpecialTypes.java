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
package org.neo4j.internal.schema.constraints;

public enum SpecialTypes implements TypeRepresentation {
    ANY("ANY", Ordering.ANY_ORDER),
    LIST_NOTHING("LIST<NOTHING>", Ordering.LIST_NOTHING_ORDER),
    LIST_ANY("LIST<ANY>", Ordering.LIST_ANY_ORDER),
    NULL("NULL", Ordering.NULL_ORDER);

    private final String userDescription;
    private final Ordering order;

    SpecialTypes(String userDescription, Ordering order) {
        this.userDescription = userDescription;
        this.order = order;
    }

    @Override
    public String userDescription() {
        return userDescription;
    }

    @Override
    public Ordering order() {
        return order;
    }
}
