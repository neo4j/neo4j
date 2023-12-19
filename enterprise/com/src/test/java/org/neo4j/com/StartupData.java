/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import java.io.Serializable;

class StartupData implements Serializable
{
    private static final long serialVersionUID = 3570271945897559074L;

    long creationTime;
    long storeId;
    byte applicationProtocolVersion;
    byte internalProtocolVersion;
    int chunkSize;

    StartupData( long creationTime, long storeId, byte internalProtocolVersion, byte applicationProtocolVersion,
            int chunkSize )
    {
        this.creationTime = creationTime;
        this.storeId = storeId;
        this.internalProtocolVersion = internalProtocolVersion;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.chunkSize = chunkSize;
    }
}
