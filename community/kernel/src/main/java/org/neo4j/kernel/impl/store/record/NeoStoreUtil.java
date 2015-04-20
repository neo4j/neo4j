/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result;

import static java.lang.String.format;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.kernel.impl.store.NeoStore.RECORD_SIZE;
import static org.neo4j.kernel.impl.store.NeoStore.TYPE_DESCRIPTOR;

/**
 * Reads the contents of a {@link NeoStore neostore} store. Namely all of its {@link Position records}
 * and makes those {@link #getValue(Position) available} for viewing.
 */
public class NeoStoreUtil
{
    private final Map<Position,Long> values = new HashMap<>();

    public static void main( String[] args )
    {
        if ( args.length < 1 )
        {
            System.err.println( "Supply one argument which is the store directory of a neo4j graph database" );
            System.exit( 1 );
        }
        System.out.println( new NeoStoreUtil( new File( args[0] ) ) );
    }

    public static boolean neoStoreExists( FileSystemAbstraction fs, File storeDir )
    {
        return fs.fileExists( neoStoreFile( storeDir, StoreFileType.STORE ) );
    }

    public NeoStoreUtil( File storeDir )
    {
        this( storeDir, new DefaultFileSystemAbstraction() );
    }

    public NeoStoreUtil( File storeDir, FileSystemAbstraction fs )
    {
        File neoStoreFile = neoStoreFile( storeDir, StoreFileType.STORE );
        String currentTypeDescriptorAndVersion = NeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
        boolean storeHasTrailer = hasTrailer( neoStoreFile, fs, currentTypeDescriptorAndVersion );
        try ( StoreChannel channel = fs.open( neoStoreFile, "r" ) )
        {
            int contentSize = (int) channel.size();
            if ( storeHasTrailer )
            {
                int trailerSize = encode( currentTypeDescriptorAndVersion ).length;
                contentSize -= trailerSize;
            }
            int records = contentSize/RECORD_SIZE;
            ByteBuffer buf = ByteBuffer.allocate( records * RECORD_SIZE );
            channel.read( buf );
            buf.flip();

            for ( int i = 0; buf.remaining() >= RECORD_SIZE && i < Position.values().length; i++ )
            {
                values.put( Position.values()[i], nextRecord( buf ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private boolean hasTrailer( File neoStoreFile, FileSystemAbstraction fs, String currentTypeDescriptorAndVersion )
    {
        StoreVersionCheck trailerCheck = new StoreVersionCheck( fs );
        Result result = trailerCheck.hasVersion( neoStoreFile, currentTypeDescriptorAndVersion );
        return result.outcome == Result.Outcome.ok || result.outcome == Result.Outcome.unexpectedUpgradingStoreVersion;
    }

    private long nextRecord( ByteBuffer buf )
    {
        buf.get(); // in use byte
        return buf.getLong();
    }

    /**
     * Returns the record value for the given {@link Position position}.
     *
     * @param position record to return value for.
     * @return the {@code long} value read from the record specified by {@code position}.
     * @throws IllegalStateException if the neostore that these values were read from didn't have
     * the record specified by {@link Position position}.
     */
    public long getValue( Position position )
    {
        Long value = values.get( position );
        if ( value == null )
        {
            throw new IllegalStateException( "Wanted record " + position +
                    ", but this record wasn't read since the neostore didn't contain it" );
        }
        return value.longValue();
    }

    public long getCreationTime()
    {
        return getValue( Position.TIME );
    }

    public long getStoreId()
    {
        return getValue( Position.RANDOM_NUMBER );
    }

    public long getLastCommittedTx()
    {
        return getValue( Position.LAST_TRANSACTION_ID );
    }

    public long getLogVersion()
    {
        return getValue( Position.LOG_VERSION );
    }

    public long getStoreVersion()
    {
        return getValue( Position.STORE_VERSION );
    }

    public long getFirstGraphProp()
    {
        return getValue( Position.FIRST_GRAPH_PROPERTY );
    }

    public long getLastCommittedTxChecksum()
    {
        return getValue( Position.LAST_TRANSACTION_CHECKSUM );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "Neostore contents:%n" );
        int i = 0;
        for ( Position position : Position.values() )
        {
            Long value = values.get( position );
            if ( value != null )
            {
                builder.append( i++ ).append( ": " ).append( position.description() )
                       .append( ": " ).append( value ).append( "%n" );
            }
        }
        builder.append( "=> store id: " )
               .append( new StoreId( getCreationTime(), getStoreId(), getStoreVersion(), -1, -1 ) );
        return format( builder.toString() );
    }

    public static boolean storeExists( File storeDir )
    {
        return storeExists( storeDir, new DefaultFileSystemAbstraction() );
    }

    public static boolean storeExists( File storeDir, FileSystemAbstraction fs )
    {
        return fs.fileExists( neoStoreFile( storeDir, StoreFileType.STORE ) );
    }

    private static File neoStoreFile( File storeDir, StoreFileType type )
    {
        return new File( storeDir, type.augment( NeoStore.DEFAULT_NAME ) );
    }
}
