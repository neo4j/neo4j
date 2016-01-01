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
            System.err.println( "SYNTAX: [file[:id[,id]*]]+" );
            return;
        }
        StoreFactory storeFactory = new StoreFactory(
                new Config(), new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                new DefaultFileSystemAbstraction(), logger(), null );
        for ( String arg : args )
        {
            File file = new File( arg );
            long[] ids = null; // null means all possible ids

            if( file.isFile() )
            {
                /* If file exists, even with : in its path, then accept it straight off. */
            }
            else if ( !file.isDirectory() && file.getName().indexOf( ':' ) != -1 )
            {
                /* Now we know that it is not a directory either, and that the last component
                   of the path contains a colon, thus it is very likely an attempt to use the
                   id-specifying syntax. */

                int idStart = arg.lastIndexOf( ':' );

                String[] idStrings = arg.substring( idStart + 1 ).split( "," );
                ids = new long[idStrings.length];
                for ( int i = 0; i < ids.length; i++ )
                {
                    ids[i] = Long.parseLong( idStrings[i] );
                }
                file = new File( arg.substring( 0, idStart ) );

                if ( !file.isFile() )
                {
                    throw new IllegalArgumentException( "No such file: " + arg );
                }
            }
            switch ( file.getName() )
            {
            case "neostore.nodestore.db":
                dumpNodeStore( file, storeFactory, ids );
                break;
            case "neostore.relationshipstore.db":
                dumpRelationshipStore( file, storeFactory, ids );
                break;
            case "neostore.propertystore.db":
                dumpPropertyStore( file, storeFactory, ids );
                break;
            case "neostore.schemastore.db":
                dumpSchemaStore( file, storeFactory, ids );
                break;
            case "neostore.propertystore.db.index":
                dumpPropertyKeys( file, storeFactory, ids );
                break;
            case "neostore.labeltokenstore.db":
                dumpLabels( file, storeFactory, ids );
                break;
            case "neostore.relationshiptypestore.db":
                dumpRelationshipTypes( file, storeFactory, ids );
                break;
            case "neostore.relationshipgroupstore.db":
                dumpRelationshipGroups( file, storeFactory, ids );
                break;
            default:
                throw new IllegalArgumentException( "Unknown store file: " + arg );
            }
        }
    }

    private static StringLogger logger()
    {
        return Boolean.getBoolean( "logger" ) ? StringLogger.SYSTEM : StringLogger.DEV_NULL;
    }

    private static void dumpPropertyKeys( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        dumpTokens( storeFactory.newPropertyKeyTokenStore( file ), ids );
    }

    private static void dumpLabels( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        dumpTokens( storeFactory.newLabelTokenStore( file ), ids );
    }

    private static void dumpRelationshipTypes( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        dumpTokens( storeFactory.newRelationshipTypeTokenStore( file ), ids );
    }

    private static <T extends TokenRecord> void dumpTokens( final TokenStore<T> store, long[] ids ) throws Exception
    {
        try
        {
            new DumpStore<T, TokenStore<T>>( System.out )
            {
                @Override
                protected Object transform( T record ) throws Exception
                {
                    if ( record.inUse() )
                    {
                        store.ensureHeavy( record );
                        return record.getId() + ": \"" + store.getStringFor( record ) + "\": " + record;
                    }
                    return null;
                }
            }.dump( store, ids );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpRelationshipGroups( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        RelationshipGroupStore store = storeFactory.newRelationshipGroupStore( file );
        try
        {
            new DumpStore<RelationshipGroupRecord, RelationshipGroupStore>( System.out ).dump( store, ids );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpRelationshipStore( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        RelationshipStore store = storeFactory.newRelationshipStore( file );
        try
        {
            new DumpStore<RelationshipRecord, RelationshipStore>( System.out ).dump( store, ids );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpPropertyStore( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        PropertyStore store = storeFactory.newPropertyStore( file );
        try
        {
            new DumpStore<PropertyRecord, PropertyStore>( System.out ).dump( store, ids );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpSchemaStore( File file, StoreFactory storeFactory, long ids[] ) throws Exception
    {
        SchemaStore store = storeFactory.newSchemaStore( file );
        try
        {
            final SchemaStorage storage = new SchemaStorage( store );
            new DumpStore<DynamicRecord, SchemaStore>( System.out )
            {
                @Override
                protected Object transform( DynamicRecord record ) throws Exception
                {
                    return record.inUse() && record.isStartRecord()
                           ? storage.loadSingleSchemaRule( record.getId() )
                           : null;
                }
            }.dump( store, ids );
        }
        finally
        {
            store.close();
        }
    }

    private static void dumpNodeStore( File file, StoreFactory storeFactory, long[] ids ) throws Exception
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
            }.dump( store, ids );
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

    public final void dump( STORE store, long[] ids ) throws Exception
    {
        store.makeStoreOk();
        int size = store.getRecordSize();
        StoreChannel fileChannel = store.getFileChannel();
        ByteBuffer buffer = ByteBuffer.allocate( size );
        out.println( "store.getRecordSize() = " + size );
        out.println( "<dump>" );
        long used = 0;

        if ( ids == null )
        {
            long high = store.getHighestPossibleIdInUse();

            for ( long id = 1; id <= high; id++ )
            {
                boolean inUse = dumpRecord( store, size, fileChannel, buffer, id );

                if ( inUse )
                {
                    used++;
                }
            }
        }
        else
        {
            for( long id : ids )
            {
                dumpRecord( store, size, fileChannel, buffer, id );
            }
        }
        out.println( "</dump>" );

        if ( ids == null )
        {
            out.printf( "used = %s / highId = %s (%.2f%%)%n", used, store.getHighId(), used * 100.0 / store.getHighId
                    () );

        }
    }

    private boolean dumpRecord( STORE store, int size, StoreChannel fileChannel, ByteBuffer buffer, long id ) throws
            Exception
    {
        RECORD record = store.forceGetRecord( id );

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
            fileChannel.read( buffer, id * size );
            buffer.flip();
            if ( record.inUse() )
            {
                dumpHex( buffer, id * size );
            }
            else if ( allZero( buffer ) )
            {
                out.printf( ": all zeros @ 0x%x - 0x%x%n", id * size, (id + 1) * size );
            }
            else
            {
                dumpHex( buffer, id * size );
            }
        }

        return record.inUse();
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
