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
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.HashMap;
import java.util.Map;

public class InMemoryStoreStreamProvider implements StoreFileStreamProvider
{
    private Map<String,StringBuffer> fileStreams = new HashMap<>();

    @Override
    public StoreFileStream acquire( String destination, int requiredAlignment )
    {
        fileStreams.putIfAbsent( destination, new StringBuffer() );
        return new InMemoryStoreStream( fileStreams.get( destination ) );
    }

    public Map<String,StringBuffer> fileStreams()
    {
        return fileStreams;
    }

    class InMemoryStoreStream implements StoreFileStream
    {
        private StringBuffer stringBuffer;

        InMemoryStoreStream( StringBuffer stringBuffer )
        {
            this.stringBuffer = stringBuffer;
        }

        public void write( byte[] data )
        {
            for ( byte b : data )
            {
                stringBuffer.append( (char) b );
            }
        }

        @Override
        public void close()
        {
            // do nothing
        }
    }
}
