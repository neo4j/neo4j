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
