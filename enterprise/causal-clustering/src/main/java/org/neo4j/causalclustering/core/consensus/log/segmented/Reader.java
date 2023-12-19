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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;

public class Reader implements Closeable
{
    private final long version;
    private final StoreChannel storeChannel;
    private long timeStamp;

    Reader( FileSystemAbstraction fsa, File file, long version ) throws IOException
    {
        this.storeChannel = fsa.open( file, OpenMode.READ );
        this.version = version;
    }

    public long version()
    {
        return version;
    }

    public StoreChannel channel()
    {
        return storeChannel;
    }

    @Override
    public void close() throws IOException
    {
        storeChannel.close();
    }

    void setTimeStamp( long timeStamp )
    {
        this.timeStamp = timeStamp;
    }

    long getTimeStamp()
    {
        return timeStamp;
    }
}
