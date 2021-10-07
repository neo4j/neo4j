/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.storageengine.migration;

public interface RollingUpgradeCompatibility
{
    /**
     * Check if formats are compatible for using in rolling upgrade
     * @param format The format to compare with (older)
     * @param otherFormat The format to compare against (newer)
     * @return true if they are compatible, false otherwise
     */
    boolean isVersionCompatibleForRollingUpgrade( String format, String otherFormat );

    /**
     * See {@link #isVersionCompatibleForRollingUpgrade(String, String)}
     */
    boolean isVersionCompatibleForRollingUpgrade( long format, long otherFormat );
}
