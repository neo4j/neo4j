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

import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionLongToString;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;

public class IndexProviderStore
{
    private static final int RECORD_SIZE = 8;
    private static final int RECORD_COUNT = 5;
    
    private final long creationTime;
    private final long randomIdentifier;
    private long version;
    private final long indexVersion;
    
    private final FileChannel fileChannel;
    private final ByteBuffer buf = ByteBuffer.allocate( RECORD_SIZE*RECORD_COUNT );
    private long lastCommittedTx;
    private final File file;
    
    public IndexProviderStore( File file, FileSystemAbstraction fileSystem, long expectedVersion, boolean allowUpgrade )
    {
        this.file = file;
        try
        {
            // Create it if it doesn't exist
            if ( !fileSystem.fileExists( file.getAbsolutePath() ) )
                create( file, fileSystem, expectedVersion );
            
            // Read all the records in the file
            fileChannel = fileSystem.open( file.getAbsolutePath(), "rw" );
            Long[] records = readRecordsWithNullDefaults( fileChannel, RECORD_COUNT, allowUpgrade ?
                    RECORD_COUNT-1 : // 1.6 introduced indexVersion record. This (-1 expectation) isn't a generic solution though
                    RECORD_COUNT );
            creationTime = records[0].longValue();
            randomIdentifier = records[1].longValue();
            version = records[2].longValue();
            lastCommittedTx = records[3].longValue();
            Long readIndexVersion = records[4];
            
            // Compare version and throw exception if there's a mismatch, also considering "allow upgrade"
            compareExpectedVersionWithStoreVersion( expectedVersion, allowUpgrade, readIndexVersion );
            
            // Here we know that either the version matches or we just upgraded to the expected version
            indexVersion = expectedVersion;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void compareExpectedVersionWithStoreVersion( long expectedVersion,
            boolean allowUpgrade, Long readIndexVersion )
    {
        boolean versionDiffers = readIndexVersion == null || readIndexVersion.longValue() != expectedVersion;
        if ( versionDiffers )
        {
            // We can throw a more explicit exception if we see that we're trying to run
            // with an older version than the store is.
            if ( readIndexVersion != null && expectedVersion < readIndexVersion.longValue() )
            {
                String expected = versionLongToString( expectedVersion );
                String readVersion = versionLongToString( readIndexVersion.longValue() );
                throw new NotCurrentStoreVersionException( expected, readVersion,
                        "Your index has been upgraded to " + readVersion +
                        " and cannot run with an older version " + expected, false );
            }
            else if ( !allowUpgrade )
            {
                // We try to run with a newer version than the store is but isn't allowed to upgrade.
                throw new UpgradeNotAllowedByConfigurationException();
            }
        }
        
        if ( versionDiffers )
        {
            // We have upgraded the version, let's write it
            writeOut();
        }
    }
    
    private Long[] readRecordsWithNullDefaults( FileChannel fileChannel, int count, int expectAtLeastCount ) throws IOException
    {
        buf.clear();
        int bytesRead = fileChannel.read( buf );
        int wholeRecordsRead = bytesRead/RECORD_SIZE;
        if ( wholeRecordsRead < expectAtLeastCount )
            throw new RuntimeException( "Expected to read at least " + expectAtLeastCount + " records, but could only read " +
                    wholeRecordsRead + " (" + bytesRead + "b)" );
        
        buf.flip();
        Long[] result = new Long[count];
        for ( int i = 0; i < wholeRecordsRead; i++ )
            result[i] = buf.getLong();
        return result;
    }
    
    private void create( File file, FileSystemAbstraction fileSystem, long indexVersion ) throws IOException
    {
        if ( fileSystem.fileExists( file.getAbsolutePath() ) )
            throw new IllegalArgumentException( file + " already exist" );
        
        FileChannel fileChannel = null;
        try
        {
            fileChannel = fileSystem.open( file.getAbsolutePath(), "rw" );
            write( fileChannel, System.currentTimeMillis(), new Random( System.currentTimeMillis() ).nextLong(),
                    0, 1, indexVersion );
        }
        finally
        {
            fileChannel.close();
        }
    }

    private void write( FileChannel channel, long time, long identifier, long version, long lastCommittedTxId,
            long indexVersion ) throws IOException
    {
        buf.clear();
        buf.putLong( time ).putLong( identifier ).putLong( version ).putLong( lastCommittedTxId ).putLong( indexVersion );
        buf.flip();
        channel.position( 0 );

        int written = channel.write( buf );
        int expectedLength = RECORD_COUNT*RECORD_SIZE;
        if ( written != expectedLength )
            throw new RuntimeException( "Expected to write " + expectedLength + " bytes, but wrote " + written );
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
    
    public long getLastCommittedTx()
    {
        return this.lastCommittedTx;
    }
    
    private void writeOut()
    {
        try
        {
            write( fileChannel, creationTime, randomIdentifier, version, lastCommittedTx, indexVersion );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void close()
    {
        if ( !fileChannel.isOpen() )
            return;
        
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