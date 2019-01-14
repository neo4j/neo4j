/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
