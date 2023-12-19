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
package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;

public final class BranchedStoreInfo implements Serializable
{
    private static final long serialVersionUID = -3519343870927764106L;

    private String directory;

    private long largestTxId;
    private long creationTime;
    private long branchedStoreSize;

    @ConstructorProperties( {"directory", "largestTxId", "creationTime"} )
    public BranchedStoreInfo( String directory, long largestTxId, long creationTime )
    {
        this( directory, largestTxId, creationTime, 0 );
    }

    @ConstructorProperties( {"directory", "largestTxId", "creationTime", "storeSize"} )
    public BranchedStoreInfo( String directory, long largestTxId, long creationTime, long branchedStoreSize )
    {
        this.directory = directory;
        this.largestTxId = largestTxId;
        this.creationTime = creationTime;
        this.branchedStoreSize = branchedStoreSize;
    }

    public String getDirectory()
    {
        return directory;
    }

    public long getLargestTxId()
    {
        return largestTxId;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getBranchedStoreSize()
    {
        return branchedStoreSize;
    }
}
