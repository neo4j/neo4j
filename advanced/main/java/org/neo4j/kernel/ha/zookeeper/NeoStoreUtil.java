/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha.zookeeper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NeoStoreUtil
{
    private static final int RECORD_SIZE = 9;

    private final long creationTime;
    private final long storeId;
    private final long txId;
    private final long version;
    
    public NeoStoreUtil( String storeDir )
    {
        try
        {
            FileChannel fileChannel = new RandomAccessFile( storeDir + "/neostore", "r" ).getChannel();
            ByteBuffer buf = ByteBuffer.allocate( 4*9 );
            if ( fileChannel.read( buf ) != 4*9 )
            {
                throw new RuntimeException( "Unable to read neo store header information" );
            }
            buf.flip();
            buf.get(); // in use byte
            creationTime = buf.getLong();
            buf.get(); // in use byte
            storeId = buf.getLong();
            buf.get(); 
            version = buf.getLong(); // skip log version
            buf.get(); // in use byte
            txId = buf.getLong();
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public long getCreationTime()
    {
        return creationTime;
    }
    
    public long getStoreId()
    {
        return storeId;
    }
    
    public long getLastCommittedTx()
    {
        return txId;
    }
    
    public long getVersion()
    {
        return version;
    }
}
