/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.kernel.DefaultFileSystemAbstraction;

public class NeoStoreUtil
{
    private final long creationTime;
    private final long randomId;
    private final long txId;
    private final long logVersion;
    private final long storeVersion;
    private final long firstGraphProp;

    public static void main( String[] args )
    {
        if ( args.length < 1 )
        {
            System.err.println( "Supply one argument which is the store directory of a neo4j graph database" );
            System.exit( 1 );
        }
        System.out.println( new NeoStoreUtil( new File( args[0] ) ) );
    }

    public NeoStoreUtil( File storeDir )
    {
        this( storeDir, new DefaultFileSystemAbstraction() );
    }

    public NeoStoreUtil( File storeDir, FileSystemAbstraction fs )
    {
        try ( StoreChannel channel = fs.open( neoStoreFile( storeDir ), "r" ) )
        {
            int recordsToRead = 6;
            int bytesToRead = recordsToRead * NeoStore.RECORD_SIZE;
            ByteBuffer buf = ByteBuffer.allocate( bytesToRead );
            int bytesRead = channel.read( buf );
            if ( bytesRead != bytesToRead )
            {
                throw new RuntimeException( "Unable to read neo store header information, " +
                        "read: " + bytesRead + "B, expected " + bytesToRead + "B");
            }
            buf.flip();
            creationTime = nextRecord( buf );
            randomId = nextRecord( buf );
            logVersion = nextRecord( buf );
            txId = nextRecord( buf );
            storeVersion = nextRecord( buf );
            firstGraphProp = nextRecord( buf );
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
        return randomId;
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

    @Override
    public String toString()
    {
        return format( "Neostore contents:%n" +
                "0: creation time: %s%n" +
                "1: random id: %s%n" +
                "2: log version: %s%n" +
                "3: tx id: %s%n" +
                "4: store version: %s%n" +
                "5: first graph prop: %s%n" +
                " => store id: %s",

                creationTime,
                randomId,
                logVersion,
                txId,
                storeVersion,
                firstGraphProp,
                new StoreId( creationTime, randomId, storeVersion, -1, -1 ) );
    }

    public static boolean storeExists( File storeDir )
    {
        return storeExists( storeDir, new DefaultFileSystemAbstraction() );
    }

    public static boolean storeExists( File storeDir, FileSystemAbstraction fs )
    {
        return fs.fileExists( neoStoreFile( storeDir ) );
    }

    private static File neoStoreFile( File storeDir )
    {
        return new File( storeDir, NeoStore.DEFAULT_NAME );
    }
}
