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
package org.neo4j.dbms.database;

/**
 * These components have 'real' versions, and may have new version numbers in later releases. As opposed to some
 * components (probably present only in Community), which just use the state of the system graph to determine if
 * there is an upgrade possible.
 */
public interface SystemGraphComponentWithVersion extends SystemGraphComponent {

    /**
     * Return the latest version of this component which the local instance can support.
     *
     * Note: this is not the same as the version of the component which is "installed" in the system database.
     */
    int getLatestSupportedVersion();
}
