/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class NeoStoreUtil
{
    private final long creationTime;
    private final long storeId;
    private final long txId;
    private final long logVersion;
    private final long storeVersion;
    
    public NeoStoreUtil( File storeDir )
    {
        try
        {
            FileChannel fileChannel = new RandomAccessFile( neoStoreFile( storeDir ), "r" ).getChannel();
            int recordsToRead = 5;
            ByteBuffer buf = ByteBuffer.allocate( recordsToRead*NeoStore.RECORD_SIZE );
            if ( fileChannel.read( buf ) != recordsToRead*NeoStore.RECORD_SIZE )
            {
                throw new RuntimeException( "Unable to read neo store header information" );
            }
            buf.flip();
            creationTime = nextRecord( buf );
            storeId = nextRecord( buf );
            logVersion = nextRecord( buf );
            txId = nextRecord( buf );
            storeVersion = nextRecord( buf );
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private long nextRecord( ByteBuffer buf )
    {
        buf.get(); // in use byte
        return buf.getLong();
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
    
    public long getLogVersion()
    {
        return logVersion;
    }
    
    public long getStoreVersion()
    {
        return storeVersion;
    }
    
    public StoreId asStoreId()
    {
        return new StoreId( creationTime, storeId, storeVersion );
    }

    public static boolean storeExists( File storeDir )
    {
        return neoStoreFile( storeDir ).exists();
    }

    private static File neoStoreFile( File storeDir )
    {
        return new File( storeDir, NeoStore.DEFAULT_NAME );
    }
}
