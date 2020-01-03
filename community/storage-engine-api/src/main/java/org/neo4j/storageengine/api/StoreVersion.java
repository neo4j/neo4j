/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.util.Optional;

import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public interface StoreVersion
{
    String storeVersion();

    boolean hasCapability( Capability capability );

    boolean hasCompatibleCapabilities( StoreVersion otherVersion, CapabilityType type );

    /**
     * @return the neo4j version where this format was introduced. It is almost certainly NOT the only version of
     * neo4j where this format is used.
     */
    String introductionNeo4jVersion();

    Optional<StoreVersion> successor();

    boolean isCompatibleWith( StoreVersion otherVersion );
}
