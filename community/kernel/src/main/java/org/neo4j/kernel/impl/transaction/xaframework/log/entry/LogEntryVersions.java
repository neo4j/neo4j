/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

public class LogEntryVersions
{
    private LogEntryVersions()
    {
        // no instances are allowed
    }

    /*
     * version 0 for Neo4j versions < 2.1
     * version -1 for Neo4j 2.1
     */
    public static final byte LEGACY_LOG_ENTRY_VERSION = (byte) 0;
    public static final byte CURRENT_LOG_ENTRY_VERSION = (byte) -1;
}
