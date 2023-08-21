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
package org.neo4j.server.http.cypher.format.jolt.v1;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Object Mapper configured to write results using the Jolt format.
 * Jolt typically produces results in the format: {@code {<type> : <value>} }.
 * For example: {@code {"Z": 1}} where "Z" indicates the value is an integer.
 */
public class JoltV1Codec extends ObjectMapper {
    /**
     * Construct a codec with strict mode enabled/disabled depending on {@code strictModeEnabled}. When strict
     * mode is enabled, values are <em>always</em> paired with their type whereas when disabled some type information
     * is omitted for brevity.
     * @param strictModeEnabled {@code true} to enable strict mode, {@code false} to disable strict mode.
     */
    public JoltV1Codec(boolean strictModeEnabled) {
        if (strictModeEnabled) {
            registerModules(JoltModuleV1.STRICT.getInstance());
        } else {
            registerModules(JoltModuleV1.DEFAULT.getInstance());
        }
    }

    /**
     * Constuct a codec with strict mode disabled.
     */
    public JoltV1Codec() {
        registerModules(JoltModuleV1.DEFAULT.getInstance());
    }
}
