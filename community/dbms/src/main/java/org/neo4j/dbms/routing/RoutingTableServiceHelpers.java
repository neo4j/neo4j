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
package org.neo4j.dbms.routing;

import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Optional;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.helpers.SocketAddressParser;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.InternalLog;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public class RoutingTableServiceHelpers {
    public static final String ADDRESS_CONTEXT_KEY = "address";
    public static final String FROM_ALIAS_KEY = "alias";

    public static Optional<SocketAddress> findClientProvidedAddress(
            MapValue routingContext, int defaultBoltPort, InternalLog log) throws RoutingException {
        var address = routingContext.get(ADDRESS_CONTEXT_KEY);
        if (address == null || address == NO_VALUE) {
            return Optional.empty();
        }

        if (address instanceof TextValue) {
            try {
                String clientProvidedAddress = ((TextValue) address).stringValue();
                if (clientProvidedAddress != null
                        && !clientProvidedAddress.isEmpty()
                        && !clientProvidedAddress.isBlank()) {
                    return Optional.of(SocketAddressParser.socketAddress(
                            clientProvidedAddress, defaultBoltPort, SocketAddress::new));
                }
                // fall through to the procedure Exception
            } catch (Exception e) { // Do nothing but warn
                log.warn("Exception attempting to determine address value from routing context", e);
            }
        }

        throw new RoutingException(
                Status.Procedure.ProcedureCallFailed,
                "An address key is included in the query string provided to the "
                        + "GetRoutingTableProcedure, but its value could not be parsed.");
    }

    public static RoutingException databaseNotFoundException(String databaseName) {
        return new RoutingException(
                DatabaseNotFound,
                "Unable to get a routing table for database '" + databaseName
                        + "' because this database does not exist");
    }

    public static RoutingException databaseNotAvailableException(String databaseName) {
        return new RoutingException(
                DatabaseUnavailable,
                "Unable to get a routing table for database '" + databaseName
                        + "' because this database is unavailable");
    }
}
