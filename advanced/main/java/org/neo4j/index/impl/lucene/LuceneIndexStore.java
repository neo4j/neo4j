/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

class LuceneIndexStore
{
    private static final int FILE_LENGTH = 8*4;
    
    private long creationTime;
    private long randomIdentifier;
    private long version;
    
    private final FileChannel fileChannel;
    private final ByteBuffer buf = ByteBuffer.allocate( FILE_LENGTH );
    private long lastCommittedTx;
    
    public LuceneIndexStore( String store )
    {
        if ( !new File( store ).exists() )
        {
            create( store );
        }
        try
        {
            fileChannel = new RandomAccessFile( store, "rw" ).getChannel();
            int bytesRead = fileChannel.read( buf );
            if ( bytesRead != FILE_LENGTH && bytesRead != FILE_LENGTH-8 )
            {
                throw new RuntimeException( "Expected to read " + FILE_LENGTH +
                        " or " + (FILE_LENGTH-8) + " bytes" );
            }
            buf.flip();
            creationTime = buf.getLong();
            randomIdentifier = buf.getLong();
            version = buf.getLong();
            lastCommittedTx = bytesRead == FILE_LENGTH ? buf.getLong() : 1;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    static void create( String store )
    {
        if ( new File( store ).exists() )
        {
            throw new IllegalArgumentException( store + " already exist" );
        }
        try
        {
            FileChannel fileChannel = 
                new RandomAccessFile( store, "rw" ).getChannel();
            ByteBuffer buf = ByteBuffer.allocate( FILE_LENGTH );
            long time = System.currentTimeMillis();
            long identifier = new Random( time ).nextLong();
            buf.putLong( time ).putLong( identifier ).putLong( 0 ).putLong( 1 );
            buf.flip();
            writeBuffer( fileChannel, buf );
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void writeBuffer( FileChannel fileChannel, ByteBuffer buf ) throws IOException
    {
        if ( fileChannel.write( buf ) != FILE_LENGTH )
        {
            throw new RuntimeException( "Expected to write " + FILE_LENGTH + " bytes" );
        }
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getRandomNumber()
    {
        return randomIdentifier;
    }

    public long getVersion()
    {
        return version;
    }

    public synchronized long incrementVersion()
    {
        long current = getVersion();
        version++;
        writeOut();
        return current;
    }

    public synchronized void setVersion( long version )
    {
        this.version = version;
        writeOut();
    }
    
    public synchronized void setLastCommittedTx( long txId )
    {
        this.lastCommittedTx = txId;
//        writeOut();
    }
    
    public long getLastCommittedTx()
    {
        return this.lastCommittedTx;
    }
    
    private void writeOut()
    {
        buf.clear();
        buf.putLong( creationTime ).putLong( randomIdentifier ).putLong( 
            version ).putLong( lastCommittedTx );
        buf.flip();
        try
        {
            fileChannel.position( 0 );
            writeBuffer( fileChannel, buf );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void close()
    {
        if ( !fileChannel.isOpen() )
        {
            return;
        }
        
        try
        {
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}