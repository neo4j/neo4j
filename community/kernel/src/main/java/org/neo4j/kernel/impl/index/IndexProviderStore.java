/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.Random;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;

import static org.neo4j.kernel.impl.store.MetaDataStore.versionLongToString;

public class IndexProviderStore
{
    private static final int RECORD_SIZE = 8;
    private static final int RECORD_COUNT = 5;

    private final long creationTime;
    private final long randomIdentifier;
    private volatile long version;
    private final long indexVersion;

    private final StoreChannel fileChannel;
    private final ByteBuffer buf = ByteBuffer.allocate( RECORD_SIZE * RECORD_COUNT );
    private volatile long lastCommittedTx;
    private final File file;
    private final Random random;

    public IndexProviderStore( File file, FileSystemAbstraction fileSystem, long expectedVersion, boolean allowUpgrade )
    {
        this.file = file;
        this.random = new Random( System.currentTimeMillis() );
        StoreChannel channel = null;
        boolean success = false;
        try
        {
            // Create it if it doesn't exist
            if ( !fileSystem.fileExists( file ) || fileSystem.getFileSize( file ) == 0 )
            {
                create( file, fileSystem, expectedVersion );
            }

            // Read all the records in the file
            channel = fileSystem.open( file, OpenMode.READ_WRITE );
            Long[] records = readRecordsWithNullDefaults( channel, RECORD_COUNT, allowUpgrade );
            creationTime = records[0];
            randomIdentifier = records[1];
            version = records[2];
            lastCommittedTx = records[3];
            Long readIndexVersion = records[4];
            fileChannel = channel;

            // Compare version and throw exception if there's a mismatch, also considering "allow upgrade"
            boolean versionDiffers = compareExpectedVersionWithStoreVersion( expectedVersion, allowUpgrade, readIndexVersion );

            // Here we know that either the version matches or we just upgraded to the expected version
            indexVersion = expectedVersion;
            if ( versionDiffers )
            // We have upgraded the version, let's write it
            {
                writeOut();
            }
            success = true;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if ( !success && channel != null )
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {   // What to do?
                }
            }
        }
    }

    private boolean compareExpectedVersionWithStoreVersion( long expectedVersion,
            boolean allowUpgrade, Long readIndexVersion )
    {
        boolean versionDiffers = readIndexVersion == null || readIndexVersion != expectedVersion;
        if ( versionDiffers )
        {
            // We can throw a more explicit exception if we see that we're trying to run
            // with an older version than the store is.
            if ( readIndexVersion != null && expectedVersion < readIndexVersion )
            {
                String expected = versionLongToString( expectedVersion );
                String readVersion = versionLongToString( readIndexVersion );
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
        return versionDiffers;
    }

    private Long[] readRecordsWithNullDefaults( StoreChannel fileChannel, int count, boolean allowUpgrade ) throws IOException
    {
        buf.clear();
        int bytesRead = fileChannel.read( buf );
        int wholeRecordsRead = bytesRead / RECORD_SIZE;
        if ( wholeRecordsRead < RECORD_COUNT && !allowUpgrade )
        {
            throw new UpgradeNotAllowedByConfigurationException( "Index version (managed by " + file + ") has changed and needs to be upgraded" );
        }

        buf.flip();
        Long[] result = new Long[count];
        for ( int i = 0; i < wholeRecordsRead; i++ )
        {
            result[i] = buf.getLong();
        }
        return result;
    }

    private void create( File file, FileSystemAbstraction fileSystem, long indexVersion ) throws IOException
    {
        if ( fileSystem.fileExists( file ) && fileSystem.getFileSize( file ) > 0 )
        {
            throw new IllegalArgumentException( file + " already exist" );
        }

        try ( StoreChannel fileChannel = fileSystem.open( file, OpenMode.READ_WRITE ) )
        {
            write( fileChannel, System.currentTimeMillis(), random.nextLong(), 0, 1, indexVersion );
        }
    }

    private void write( StoreChannel channel, long time, long identifier, long version, long lastCommittedTxId,
            long indexVersion ) throws IOException
    {
        buf.clear();
        buf.putLong( time ).putLong( identifier ).putLong( version ).putLong( lastCommittedTxId ).putLong( indexVersion );
        buf.flip();

        channel.writeAll( buf, 0 );
        channel.force( true );
    }

    public File getFile()
    {
        return file;
    }

    public long getCreationTime()
    {
        return creationTime;
    }

    public long getVersion()
    {
        return version;
    }

    public long getIndexVersion()
    {
        return indexVersion;
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
