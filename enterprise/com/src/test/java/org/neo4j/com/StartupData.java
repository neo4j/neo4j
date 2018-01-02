/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import java.io.Serializable;

class StartupData implements Serializable
{
    private static final long serialVersionUID = 1L;
    final long creationTime;
    final long storeId;
    final byte applicationProtocolVersion;
    final byte internalProtocolVersion;
    final int chunkSize;

    public StartupData( long creationTime, long storeId, byte internalProtocolVersion, byte applicationProtocolVersion,
                        int chunkSize )
    {
        this.creationTime = creationTime;
        this.storeId = storeId;
        this.internalProtocolVersion = internalProtocolVersion;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.chunkSize = chunkSize;
    }
}
