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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.logging.InternalLog;

/**
 * Provides access to core DBMS components during capability registration.
 */
public final class CapabilityProviderContext {
    private final CapabilityProviderDependencies dependencies;

    CapabilityProviderContext(CapabilityProviderDependencies dependencies) {
        this.dependencies = dependencies;
    }

    /**
     * @return configuration component.
     */
    public Configuration config() {
        return get(Configuration.class);
    }

    /**
     * @return database management service component.
     */
    public DatabaseManagementService dbms() {
        return get(DatabaseManagementService.class);
    }

    /**
     * @return log component.
     */
    public InternalLog log() {
        return get(InternalLog.class);
    }

    /**
     * Return registered component of provided type.
     *
     * @param cls type of component.
     * @param <T> type parameter.
     * @return component of provided type that is registered as a dependency.
     */
    public <T> T get(Class<T> cls) {
        return dependencies.get(cls);
    }
}
