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
package org.neo4j.bolt.protocol.common.message;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum AccessMode {
    WRITE("w"),
    READ("r");

    private static final Map<String, AccessMode> flagToModeMap;

    private final String flag;

    static {
        AccessMode[] values = values();
        var mutableFlagMap = new HashMap<String, AccessMode>(1 + values.length);
        // allow "a" for AUTO access mode, which is considered WRITE for now
        mutableFlagMap.put("a", WRITE);
        for (var mode : values) {
            mutableFlagMap.put(mode.flag, mode);
        }
        flagToModeMap = Map.copyOf(mutableFlagMap);
    }

    AccessMode(String flag) {
        this.flag = flag;
    }

    public String getFlag() {
        return flag;
    }

    public static Optional<AccessMode> byFlag(String flag) {
        return Optional.ofNullable(flagToModeMap.get(flag));
    }
}
