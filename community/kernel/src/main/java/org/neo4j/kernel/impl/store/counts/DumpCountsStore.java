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
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.pagecache.StandalonePageCache;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.kvstore.MetadataVisitor;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class DumpCountsStore implements CountsVisitor, MetadataVisitor<Metadata>, UnknownKey.Visitor
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
                    .accept( new DumpCountsStore() );
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
                    life.add( tracker ).accept( new DumpCountsStore() );
                }
            }
        }
    }

    @Override
    public void visitMetadata( File path, Metadata metadata, int entryCount )
    {
        System.out.println( "Counts Store:\t" + path );
        System.out.println( "\ttxId:\t" + metadata.txId );
        System.out.println( "\tminor version:\t" + metadata.minorVersion );
        System.out.println( "\tentries:\t" + entryCount );
        System.out.println( "Entries:" );
    }

    @Override
    public void visitNodeCount( int labelId, long count )
    {
        System.out.println( "\t" + nodeKey( labelId ) + ":\t" + count );
    }

    @Override
    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        System.out.println( "\t" + relationshipKey( startLabelId, typeId, endLabelId ) + ":\t" + count );
    }

    @Override
    public void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size )
    {
        System.out.println( "\t" + indexStatisticsKey( labelId, propertyKeyId ) +
                            ":\tupdates=" + updates + ", size=" + size );
    }

    @Override
    public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        System.out.println( "\t" + indexSampleKey( labelId, propertyKeyId ) +
                            ":\tunique=" + unique + ", size=" + size );
    }

    @Override
    public boolean visitUnknownKey( ReadableBuffer key, ReadableBuffer value )
    {
        System.out.println( "\t" + key + ":\t" + value );
        return true;
    }
}
