/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;

public class DumpStore<RECORD extends AbstractBaseRecord, STORE extends CommonAbstractStore & RecordStore<RECORD>>
{
    public static void main( String... args ) throws Exception
    {
        if ( args == null || args.length == 0 )
        {
            System.err.println( "WARNING: no files specified..." );
            return;
        }
        StoreFactory storeFactory = new StoreFactory(
                new Config(), new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                new DefaultFileSystemAbstraction(), logger(), null );
        for ( String arg : args )
        {
            File file = new File( arg );
            if ( !file.isFile() )
            {
                throw new IllegalArgumentException( "No such file: " + arg );
            }
            if ( "neostore.nodestore.db".equals( file.getName() ) )
            {
                dumpNodeStore( file, storeFactory );
            }
            else if ( "neostore.relationshipstore.db".equals( file.getName() ) )
            {
                dumpRelationshipStore( file, storeFactory );
            }
            else if ( "neostore.propertystore.db".equals( file.getName() ) )
            {
                dumpPropertyStore( file, storeFactory );
            }
            else if ( "neostore.propertystore.db.index".equals( file.getName() ) )
            {
                dumpPropertyKeys( file, storeFactory );
            }
            else if ( "neostore.relationshiptypestore.db".equals( file.getName() ) )
            {
                dumpRelationshipTypes( file, storeFactory );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown store file: " + arg );
            }
        }
    }

    private static StringLogger logger()
    {
        return Boolean.getBoolean( "logger" ) ? StringLogger.SYSTEM : StringLogger.DEV_NULL;
    }

    private static void dumpPropertyKeys( File file, StoreFactory storeFactory ) throws Exception
    {
        dumpTokens( storeFactory.newPropertyIndexStore( file ) );
    }

    private static void dumpRelationshipTypes( File file, StoreFactory storeFactory ) throws Exception
    {
        dumpTokens( storeFactory.newRelationshipTypeStore( file ) );
    }

    private static <T extends AbstractNameRecord> void dumpTokens( final AbstractNameStore<T> store ) throws Exception
    {
        try
        {
            new DumpStore<T, AbstractNameStore<T>>( System.out )
            {
                @Override
                protected Object transform( T record ) throws Exception
                {
                    if ( record.inUse() )
                    {
                        store.makeHeavy( record );
                        return record.getId() + ": \"" + store.getStringFor( record ) + "\": " + record;
                    }
                    return null;
                }
            }.dump( store );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpRelationshipStore( File file, StoreFactory storeFactory ) throws Exception
    {
        RelationshipStore store = storeFactory.newRelationshipStore( file );
        try
        {
            new DumpStore<RelationshipRecord, RelationshipStore>( System.out ).dump( store );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpPropertyStore( File file, StoreFactory storeFactory ) throws Exception
    {
        PropertyStore store = storeFactory.newPropertyStore( file );
        try
        {
            new DumpStore<PropertyRecord, PropertyStore>( System.out ).dump( store );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpNodeStore( File file, StoreFactory storeFactory ) throws Exception
    {
        NodeStore store = storeFactory.newNodeStore( file );
        try
        {
            new DumpStore<NodeRecord, NodeStore>( System.out )
            {
                @Override
                protected Object transform( NodeRecord record ) throws Exception
                {
                    return record.inUse() ? record : "";
                }
            }.dump( store );
        }
        finally
        {
            store.close();
        }
    }

    private final PrintStream out;

    protected DumpStore( PrintStream out )
    {
        this.out = out;
    }

    public final void dump( STORE store ) throws Exception
    {
        store.makeStoreOk();
        int size = store.getRecordSize();
        StoreChannel fileChannel = store.getFileChannel();
        ByteBuffer buffer = ByteBuffer.allocate( size );
        out.println( "store.getRecordSize() = " + size );
        out.println( "<dump>" );
        long used = 0;
        for ( long i = 1, high = store.getHighestPossibleIdInUse(); i <= high; i++ )
        {
            RECORD record = store.forceGetRecord( i );
            if ( record.inUse() )
            {
                used++;
            }
            Object transform = transform( record );
            if ( transform != null )
            {
                if ( !"".equals( transform ) )
                {
                    out.println( transform );
                }
            }
            else
            {
                out.print( record );
                buffer.clear();
                fileChannel.read( buffer, i * size );
                buffer.flip();
                if ( record.inUse() )
                {
                    dumpHex( buffer, i * size );
                }
                else if ( allZero( buffer ) )
                {
                    out.printf( ": all zeros @ 0x%x - 0x%x%n", i * size, (i + 1) * size );
                }
                else
                {
                    dumpHex( buffer, i * size );
                }
            }
        }
        out.println( "</dump>" );
        out.printf( "used = %s / highId = %s (%.2f%%)%n", used, store.getHighId(), used * 100.0 / store.getHighId() );
    }

    private boolean allZero( ByteBuffer buffer )
    {
        int pos = buffer.position();
        try
        {
            while ( buffer.remaining() > 0 )
            {
                if ( buffer.get() != 0 )
                {
                    return false;
                }
            }
        }
        finally
        {
            buffer.position( pos );
        }
        return true;
    }

    protected Object transform( RECORD record ) throws Exception
    {
        return record.inUse() ? record : null;
    }

    private void dumpHex( ByteBuffer buffer, long offset )
    {
        for ( int count = 0; buffer.remaining() > 0; count++, offset++ )
        {
            int b = buffer.get();
            if ( count % 16 == 0 )
            {
                out.printf( "%n    @ 0x%08x: ", offset );
            }
            else if ( count % 4 == 0 )
            {
                out.print( " " );
            }
            out.printf( " %x%x", 0xF & (b >> 4), 0xF & b );
        }
        out.println();
    }
}
