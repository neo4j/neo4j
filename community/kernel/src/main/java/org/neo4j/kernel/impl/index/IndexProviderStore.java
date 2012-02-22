/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;

public class IndexProviderStore
{
    private static final int FILE_LENGTH = 8*5;
    
    private long creationTime;
    private long randomIdentifier;
    private long version;
    private long indexVersion;
    
    private final FileChannel fileChannel;
    private final ByteBuffer buf = ByteBuffer.allocate( FILE_LENGTH );
    private long lastCommittedTx;
    private final File file;
    
    public IndexProviderStore( File file, FileSystemAbstraction fileSystem, long expectedVersion, boolean allowUpgrade )
    {
        this.file = file;
        if ( !file.exists() )
        {
            create( file, fileSystem, expectedVersion );
        }
        try
        {
            fileChannel = fileSystem.open( file.getAbsolutePath(), "rw" );
            int bytesRead = fileChannel.read( buf );
            if ( bytesRead != FILE_LENGTH && bytesRead != FILE_LENGTH-8 && !allowUpgrade )
            {
                throw new RuntimeException( "Expected to read " + FILE_LENGTH +
                        " or " + (FILE_LENGTH-8) + " bytes" );
            }
            buf.flip();
            creationTime = buf.getLong();
            randomIdentifier = buf.getLong();
            version = buf.getLong();
            lastCommittedTx = bytesRead/8 >= 4 ? buf.getLong() : 1;
            Long readIndexVersion = bytesRead/8 >= 5 ? buf.getLong() : null;
            boolean versionDiffers = readIndexVersion == null || readIndexVersion.longValue() != expectedVersion;
            if ( versionDiffers && !allowUpgrade ) throw new UpgradeNotAllowedByConfigurationException();
            indexVersion = expectedVersion;
            if ( versionDiffers ) writeOut();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private void create( File file, FileSystemAbstraction fileSystem, long indexVersion )
    {
        if ( file.exists() )
        {
            throw new IllegalArgumentException( file + " already exist" );
        }
        try
        {
            FileChannel fileChannel = fileSystem.open( file.getAbsolutePath(), "rw" );
            ByteBuffer buf = ByteBuffer.allocate( FILE_LENGTH );
            long time = System.currentTimeMillis();
            long identifier = new Random( time ).nextLong();
            buf.putLong( time ).putLong( identifier ).putLong( 0 ).putLong( 1 ).putLong( indexVersion );
            buf.flip();
            writeBuffer( fileChannel, buf );
            fileChannel.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void writeBuffer( FileChannel fileChannel, ByteBuffer buf ) throws IOException
    {
        int written = fileChannel.write( buf );
        if ( written != FILE_LENGTH )
        {
            throw new RuntimeException( "Expected to write " + FILE_LENGTH + " bytes, but wrote " + written );
        }
    }
    
    public File getFile()
    {
        return file;
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
    
    public long getIndexVersion()
    {
        return indexVersion;
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
    }
    
    public synchronized void setIndexVersion( long indexVersion )
    {
        this.indexVersion = indexVersion;
        writeOut();
    }
    
    public long getLastCommittedTx()
    {
        return this.lastCommittedTx;
    }
    
    private void writeOut()
    {
        buf.clear();
        buf.putLong( creationTime ).putLong( randomIdentifier ).putLong( 
            version ).putLong( lastCommittedTx ).putLong( indexVersion );
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
        
        writeOut();
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