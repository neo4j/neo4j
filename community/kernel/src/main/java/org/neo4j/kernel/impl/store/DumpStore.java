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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;

public class DumpStore<RECORD extends AbstractBaseRecord, STORE extends CommonAbstractStore & RecordStore<RECORD>>
{
    public static void main( String... args ) throws Exception
    {
        if ( args == null || args.length == 0 )
        {
            System.err.println( "SYNTAX: [file[:id[,id]*]]+" );
            return;
        }
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        try ( PageCache pageCache = createPageCache( fs ) )
        {
            DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
            StoreFactory storeFactory = new StoreFactory( null, new Config(), idGeneratorFactory, pageCache, fs, logProvider(), null );

            for ( String arg : args )
            {
                dumpFile( storeFactory, arg );
            }
        }
    }

    private static void dumpFile( StoreFactory storeFactory, String arg ) throws Exception
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

    private static LogProvider logProvider()
    {
        return Boolean.getBoolean( "logger" ) ? FormattedLogProvider.toOutputStream( System.out ) : NullLogProvider.getInstance();
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
        try ( RelationshipGroupStore store = storeFactory.newRelationshipGroupStore( file ) )
        {
            new DumpStore<RelationshipGroupRecord,RelationshipGroupStore>( System.out ).dump( store, ids );
        }
    }

    private static void dumpRelationshipStore( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        try ( RelationshipStore store = storeFactory.newRelationshipStore( file ) )
        {
            new DumpStore<RelationshipRecord,RelationshipStore>( System.out ).dump( store, ids );
        }
    }

    private static void dumpPropertyStore( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        try ( PropertyStore store = storeFactory.newPropertyStore( file ) )
        {
            new DumpStore<PropertyRecord, PropertyStore>( System.out ).dump( store, ids );
        }
    }

    private static void dumpSchemaStore( File file, StoreFactory storeFactory, long ids[] ) throws Exception
    {
        try ( SchemaStore store = storeFactory.newSchemaStore( file ) )
        {
            final SchemaStorage storage = new SchemaStorage( store );
            new DumpStore<DynamicRecord,SchemaStore>( System.out )
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
    }

    private static void dumpNodeStore( File file, StoreFactory storeFactory, long[] ids ) throws Exception
    {
        try ( NodeStore store = storeFactory.newNodeStore( file ) )
        {
            new DumpStore<NodeRecord,NodeStore>( System.out )
            {
                @Override
                protected Object transform( NodeRecord record ) throws Exception
                {
                    return record.inUse() ? record : "";
                }
            }.dump( store, ids );
        }
    }

    private final PrintStream out;
    private final HexPrinter printer;

    protected DumpStore( PrintStream out )
    {
        this.out = out;
        this.printer = new HexPrinter( out ).withBytesGroupingFormat( 16, 4, "  " ).withLineNumberDigits( 8 );
    }

    public final void dump( STORE store, long[] ids ) throws Exception
    {
        store.makeStoreOk();
        int size = store.getRecordSize();
        out.println( "store.getRecordSize() = " + size );
        out.println( "<dump>" );
        long used = 0;
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        if ( ids == null )
        {
            long high = store.getHighestPossibleIdInUse();

            for ( long id = 0; id <= high; id++ )
            {
                boolean inUse = dumpRecord( store, size, id );

                if ( inUse )
                {
                    used++;
                }
            }
        }
        else
        {
            for ( long id : ids )
            {
                dumpRecord( store, size, id );
            }
        }
        out.println( "</dump>" );

        if ( ids == null )
        {
            out.printf( "used = %s / highId = %s (%.2f%%)%n", used, store.getHighId(),
                    used * 100.0 / store.getHighId() );
        }
    }

    private boolean dumpRecord( STORE store, int size, long id ) throws Exception
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
            byte[] data = new byte[size];
            try ( PageCursor pageCursor = store.storeFile.io( id / store.recordsPerPage(), PagedFile.PF_SHARED_LOCK ) )
            {
                if ( pageCursor.next() )
                {
                    do
                    {
                        pageCursor.setOffset( (int) (id % store.recordsPerPage() * size) );
                        pageCursor.getBytes( data );
                    }
                    while ( pageCursor.shouldRetry() );
                }
            }
            dumpHex( record, ByteBuffer.wrap( data ), id, size );
        }
        return record.inUse();
    }

    void dumpHex( RECORD record, ByteBuffer buffer, long id, int size )
    {
        printer.withLineNumberOffset( id * size );
        if ( record.inUse() )
        {
            printer.append( buffer );
        }
        else if ( allZero( buffer ) )
        {
            out.printf( ": all zeros @ 0x%x - 0x%x", id * size, (id + 1) * size );
        }
        else
        {
            printer.append( buffer );
        }
        out.printf( "%n" );
    }

    private boolean allZero( ByteBuffer buffer )
    {
        for ( int i = 0; i < buffer.limit(); i++ )
        {
            if ( buffer.get( i ) != 0 )
            {
                return false;
            }
        }
        return true;
    }

    protected Object transform( RECORD record ) throws Exception
    {
        return record.inUse() ? record : null;
    }
}
