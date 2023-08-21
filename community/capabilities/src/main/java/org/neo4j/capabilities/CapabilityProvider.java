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
package org.neo4j.capabilities;

import org.neo4j.annotations.service.Service;

/**
 * A service interface that individual components can provide in order to inject their own capabilities into the capability registry.
 */
@Service
public interface CapabilityProvider {

    /**
     * Returns the namespace into which this provider will register it's capabilities.
     *
     * @return the namespace
     */
    String namespace();

    /**
     * Registers capabilities into the registry.
     *
     * @param ctx      the context which can be used to access DBMS components during capability registration.
     * @param registry the capabilities registry instance.
     */
    void register(CapabilityProviderContext ctx, CapabilitiesRegistry registry);
}
