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
package org.neo4j.storageengine.api;

import java.util.Optional;
import org.neo4j.configuration.Config;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public interface StoreVersion extends StoreVersionUserStringProvider {

    boolean hasCapability(Capability capability);

    boolean hasCompatibleCapabilities(StoreVersion otherVersion, CapabilityType type);

    /**
     * @return the neo4j version where this format was introduced. It is almost certainly NOT the only version of
     * neo4j where this format is used.
     */
    String introductionNeo4jVersion();

    /**
     * @return if this version isn't the latest version, the returned optional will contain the store version superseding this version.
     */
    Optional<StoreVersion> successorStoreVersion(Config config);

    String formatName();

    /**
     * Some formats we just keep track of to be able to migrate from them.
     * A format that is only for migration is not supported to run a database on, and does not have support in all tools.
     */
    boolean onlyForMigration();
}
