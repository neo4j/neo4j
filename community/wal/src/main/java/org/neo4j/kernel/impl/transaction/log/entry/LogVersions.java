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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.storageengine.api.StoreId;

public class LogVersions
{
    private LogVersions()
    {
        // no instances are allowed
    }

    /**
     * Total 16 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     */
    public static final byte LOG_VERSION_3_5 = 6;

    /**
     * 3.5 log header byte size
     */
    public static final int LOG_HEADER_SIZE_3_5 = 16;

    /**
     * Total 64 bytes
     * - 8 bytes version
     * - 8 bytes last committed tx id
     * - 40 bytes {@link StoreId}
     * - 8 bytes reserved
     * <pre>
     *   |<-                      LOG_HEADER_SIZE                  ->|
     *   |<-LOG_HEADER_VERSION_SIZE->|                               |
     *   |-----------------------------------------------------------|
     *   |          version          | last tx | store id | reserved |
     *  </pre>
     */
    public static final byte LOG_VERSION_4_0 = 7;

    /**
     * 4.0 log header byte size
     */
    public static final int LOG_HEADER_SIZE_4_0 = 64;

    /**
     * Current and latest log format version
     */
    public static final byte CURRENT_LOG_FORMAT_VERSION = LOG_VERSION_4_0;

    /**
     * Current and latest header format byte size.
     */
    public static final int CURRENT_FORMAT_LOG_HEADER_SIZE = LOG_HEADER_SIZE_4_0;
}
