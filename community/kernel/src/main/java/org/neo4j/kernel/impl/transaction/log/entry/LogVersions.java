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
package org.neo4j.kernel.impl.transaction.log.entry;

public class LogVersions
{
    private LogVersions()
    {
        // no instances are allowed
    }

    /* version 1 as of 2011-02-22
     * version 2 as of 2011-10-17
     * version 3 as of 2013-02-09: neo4j 2.0 Labels & Indexing
     * version 4 as of 2014-02-06: neo4j 2.1 Dense nodes, split by type/direction into groups
     * version 5 as of 2014-05-23: neo4j 2.2 Removal of JTA / unified data source
     */
    public static final byte LOG_VERSION_1_9 = (byte) 2;
    public static final byte LOG_VERSION_2_0 = (byte) 3;
    public static final byte LOG_VERSION_2_1 = (byte) 4;
    public static final byte LOG_VERSION_2_2 = (byte) 5;
    public static final byte CURRENT_LOG_VERSION = LOG_VERSION_2_2;

    // on disk current format version
    static final short CURRENT_FORMAT_VERSION = CURRENT_LOG_VERSION & 0xFF;
}
