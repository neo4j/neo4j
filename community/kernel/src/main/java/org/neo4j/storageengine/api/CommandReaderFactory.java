/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

/**
 * Provides {@link CommandReader} instances for specific entry versions.
 */
public interface CommandReaderFactory
{
    /**
     * Given {@code version} give back a {@link CommandReader} capable of reading such commands.
     * Command writers/readers may choose to use log entry version for command versioning or could
     * introduce its own scheme.
     *
     * @param version log entry version. Versions are typically 0 or negative numbers.
     * @return {@link CommandReader} for reading commands of that version.
     */
    CommandReader byVersion( byte version );
}
