/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.tools.dump;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.util.HexPrinter;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.PrintStreamLogger;
import org.neo4j.storageengine.api.Token;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Tool to dump content of specified store into readable format for further analysis.
 * @param <RECORD> type of record to dump
 * @param <STORE> type of store to dump
 */
public class DumpStore<RECORD extends AbstractBaseRecord, STORE extends RecordStore<RECORD>>
{

    public static void main( String... args ) throws Exception
    {
        if ( args == null || args.length == 0 )
        {
            System.err.println( "SYNTAX: [file[:id[,id]*]]+" );
            return;
        }
        final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        try ( PageCache pageCache = createPageCache( fs ) )
        {
            Function<File,StoreFactory> createStoreFactory = file -> new StoreFactory( file.getParentFile(),
                    Config.defaults(), idGeneratorFactory, pageCache, fs, logProvider() );

            for ( String arg : args )
            {
                dumpFile( createStoreFactory, arg );
            }
        }
    }

    private static void dumpFile( Function<File, StoreFactory> createStoreFactory, String fileName ) throws Exception
    {
        File file = new File( fileName );
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

            int idStart = fileName.lastIndexOf( ':' );

            String[] idStrings = fileName.substring( idStart + 1 ).split( "," );
            ids = new long[idStrings.length];
            for ( int i = 0; i < ids.length; i++ )
            {
                ids[i] = Long.parseLong( idStrings[i] );
            }
            file = new File( fileName.substring( 0, idStart ) );

            if ( !file.isFile() )
            {
                throw new IllegalArgumentException( "No such file: " + fileName );
            }
        }
        StoreType storeType = StoreType.typeOf( file.getName() ).orElseThrow(
                () -> new IllegalArgumentException( "Not a store file: " + fileName ) );
        try ( NeoStores neoStores = createStoreFactory.apply( file ).openNeoStores( storeType ) )
        {
            switch ( storeType )
            {
            case META_DATA:
                dumpMetaDataStore( neoStores );
                break;
            case NODE:
                dumpNodeStore( neoStores, ids );
                break;
            case RELATIONSHIP:
                dumpRelationshipStore( neoStores, ids );
                break;
            case PROPERTY:
                dumpPropertyStore( neoStores, ids );
                break;
            case SCHEMA:
                dumpSchemaStore( neoStores, ids );
                break;
            case PROPERTY_KEY_TOKEN:
                dumpPropertyKeys( neoStores, ids );
                break;
            case LABEL_TOKEN:
                dumpLabels( neoStores, ids );
                break;
            case RELATIONSHIP_TYPE_TOKEN:
                dumpRelationshipTypes( neoStores, ids );
                break;
            case RELATIONSHIP_GROUP:
                dumpRelationshipGroups( neoStores, ids );
                break;
            default:
                throw new IllegalArgumentException( "Unsupported store type: " + storeType );
            }
        }
    }

    private static void dumpMetaDataStore( NeoStores neoStores )
    {
        neoStores.getMetaDataStore().logRecords( new PrintStreamLogger( System.out ) );
    }

    private static LogProvider logProvider()
    {
        return Boolean.getBoolean( "logger" ) ? FormattedLogProvider.toOutputStream( System.out ) : NullLogProvider.getInstance();
    }

    private static <R extends AbstractBaseRecord, S extends RecordStore<R>> void dump(
            long[] ids, S store ) throws Exception
    {
        new DumpStore<R,S>( System.out ).dump( store, ids );
    }

    private static void dumpPropertyKeys( NeoStores neoStores, long[] ids ) throws Exception
    {
        dumpTokens( neoStores.getPropertyKeyTokenStore(), ids );
    }

    private static void dumpLabels( NeoStores neoStores, long[] ids ) throws Exception
    {
        dumpTokens( neoStores.getLabelTokenStore(), ids );
    }

    private static void dumpRelationshipTypes( NeoStores neoStores, long[] ids ) throws Exception
    {
        dumpTokens( neoStores.getRelationshipTypeTokenStore(), ids );
    }

    private static <R extends TokenRecord, T extends Token> void dumpTokens(
            final TokenStore<R, T> store, long[] ids ) throws Exception
    {
        try
        {
            new DumpStore<R, TokenStore<R, T>>( System.out )
            {
                @Override
                protected Object transform( R record ) throws Exception
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

    private static void dumpRelationshipGroups( NeoStores neoStores, long[] ids ) throws Exception
    {
        dump( ids, neoStores.getRelationshipGroupStore() );
    }

    private static void dumpRelationshipStore( NeoStores neoStores, long[] ids ) throws Exception
    {
        dump( ids, neoStores.getRelationshipStore() );
    }

    private static void dumpPropertyStore( NeoStores neoStores, long[] ids ) throws Exception
    {
        dump( ids, neoStores.getPropertyStore() );
    }

    private static void dumpSchemaStore( NeoStores neoStores, long[] ids ) throws Exception
    {
        try ( SchemaStore store = neoStores.getSchemaStore() )
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

    private static void dumpNodeStore( NeoStores neoStores, long[] ids ) throws Exception
    {
        new DumpStore<NodeRecord,NodeStore>( System.out )
        {
            @Override
            protected Object transform( NodeRecord record ) throws Exception
            {
                return record.inUse() ? record : "";
            }
        }.dump( neoStores.getNodeStore(), ids );
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
        int size = store.getRecordSize();
        out.println( "store.getRecordSize() = " + size );
        out.println( "<dump>" );
        long used = 0;
        long highId = -1;

        if ( ids == null )
        {
            highId = store.getHighId();

            for ( long id = 0; id < highId; id++ )
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
            out.printf( "used = %s / highId = %s (%.2f%%)%n", used, highId, used * 100.0 / highId );
        }
    }

    private boolean dumpRecord( STORE store, int size, long id ) throws Exception
    {
        RECORD record = store.getRecord( id, store.newRecord(), FORCE );
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
            // TODO Hmm, please don't do this
            byte[] rawRecord = ((CommonAbstractStore)store).getRawRecordData( id );
            dumpHex( record, ByteBuffer.wrap( rawRecord ), id, size );
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
