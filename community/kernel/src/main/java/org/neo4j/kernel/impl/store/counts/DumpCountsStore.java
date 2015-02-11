/**
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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.pagecache.StandalonePageCache;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueVisitor;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class DumpCountsStore implements AbstractKeyValueVisitor<CountsKey, Metadata>
{
    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 1 )
        {
            System.out.println( "one argument describing the path to the store" );
            System.exit( 1 );
        }

        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File path = new File( args[0] );

        try ( StandalonePageCache pages = createPageCache( fs, "counts-store-dump" ); Lifespan life = new Lifespan() )
        {
            if ( fs.isDirectory( path ) )
            {
                life.add( new StoreFactory( fs, path, pages, DEV_NULL, new Monitors() ).newCountsStore() )
                    .visitFile( new DumpCountsStore() );
            }
            else
            {
                CountsTracker tracker = new CountsTracker( DEV_NULL, fs, pages, path );
                if ( fs.fileExists( path ) )
                {
                    tracker.visitFile( path, new DumpCountsStore() );
                }
                else
                {
                    life.add( tracker ).visitFile( new DumpCountsStore() );
                }
            }
        }
    }

    @Override
    public void visitMetadata( File path, Metadata metadata, int entryCount )
    {
        System.out.println( "Counts Store: " + path );
        System.out.println( "\ttxId: " + metadata.txId );
        System.out.println( "\tminor version: " + metadata.minorVersion );
        System.out.println( "\tentries: " + entryCount );
        System.out.println( "Entries:" );
    }

    @Override
    public void visitData( CountsKey key, ReadableBuffer value )
    {
        switch ( key.recordType() )
        {
        case ENTITY_NODE:
        case ENTITY_RELATIONSHIP:
            System.out.println( "\t" + key + ": " + value.getLong( 8 ) );
            break;
        case INDEX_STATISTICS:
        case INDEX_SAMPLE:
            System.out.println( "\t" + key + ": (" + value.getLong( 0 ) + ", " + value.getLong( 8 ) + ")" );
            break;
        default:
            throw new IllegalStateException( "Unknown or empty key: " + key );
        }

    }
}
