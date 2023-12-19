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
package org.neo4j.causalclustering.core.consensus.log;

import java.util.Objects;

public class LogPosition
{
    public long logIndex;
    public long byteOffset;

    public LogPosition( long logIndex, long byteOffset )
    {
        this.logIndex = logIndex;
        this.byteOffset = byteOffset;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        LogPosition position = (LogPosition) o;
        return logIndex == position.logIndex && byteOffset == position.byteOffset;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( logIndex, byteOffset );
    }
}
