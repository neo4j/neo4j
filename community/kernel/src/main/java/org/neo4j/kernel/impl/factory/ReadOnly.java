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
package org.neo4j.kernel.impl.factory;

import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.kernel.api.exceptions.Status;

public class ReadOnly implements AccessCapability {
    public static final ReadOnly INSTANCE = new ReadOnly();

    private ReadOnly() {}

    @Override
    public void assertCanWrite() {
        throw new WriteOperationsNotAllowedException(
                "No write operations are allowed on this database. The database is in read-only mode on this Neo4j instance.",
                Status.General.WriteOnReadOnlyAccessDatabase);
    }
}
