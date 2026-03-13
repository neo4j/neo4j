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
package org.neo4j.fleetmanagement.procedures;

import java.util.stream.Stream;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class DebugLogging {

    public static class Result {
        @Description("A message indicating the result of the operation.")
        public String result;

        public Result(String result) {
            this.result = result;
        }
    }

    @Procedure(name = "fleetManagement.debugLogging", mode = Mode.DBMS)
    @SystemProcedure
    @Admin
    @Description("Enable debug or payload logging for Fleet Manager.")
    public Stream<Result> debugLogging(
            @Name(value = "debugLoggingEnabled", description = "Enable debug logging for fleet manager on this server.")
                    Boolean enabled,
            @Name(
                            value = "payloadLoggingEnabled",
                            description = "Enable payload logging for fleet manager on this server.")
                    Boolean payloadLogging) {
        Logger.setDebugEnabled(enabled);
        Logger.setPayloadLoggingEnabled(payloadLogging);
        Logger.getNeo4jLogger().info("Debug logging enabled: %s, Payload logging enabled: %s", enabled, payloadLogging);

        return Stream.of(new Result(
                String.format("Debug logging enabled: %s, Payload logging enabled: %s", enabled, payloadLogging)));
    }
}
