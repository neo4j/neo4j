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

import java.util.Locale;

public enum CRSTable {
    CUSTOM("custom", 0),
    EPSG("epsg", 1),
    SR_ORG("sr-org", 2);

    static final CRSTable[] TYPES = CRSTable.values();

    private final String prefix;

    public static CRSTable find(int tableId) {
        if (tableId < TYPES.length) {
            return TYPES[tableId];
        } else {
            throw new IllegalArgumentException("No known Coordinate Reference System table: " + tableId);
        }
    }

    private final String name;
    private final int tableId;

    CRSTable(String name, int tableId) {
        assert lowerCase(name);
        this.name = name;
        this.tableId = tableId;
        this.prefix = tableId == 0 ? "crs://" + name + "/" : "https://spatialreference.org/ref/" + name + "/";
    }

    public String href(int code) {
        return prefix + code + "/";
    }

    private static boolean lowerCase(String string) {
        return string.toLowerCase(Locale.ROOT).equals(string);
    }

    public String getName() {
        return name;
    }

    public int getTableId() {
        return tableId;
    }
}
