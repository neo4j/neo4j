/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.IOException;

import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Common interface for the node,relationship,property and relationship type
 * stores.
 */
public interface Store
{
    /**
     * Returns the id of next free record.
     *
     * @return The id of the next free record
     * @throws IOException
     *             If unable to
     */
    public long nextId();

    public String getTypeDescriptor();

    public long getHighestPossibleIdInUse();

    public long getNumberOfIdsInUse();

    public WindowPoolStats getWindowPoolStats();

    public void logIdUsage( StringLogger.LineLogger logger );
}
