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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.pagecache.StandalonePageCache;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.register.Register.CopyableDoubleLongRegister;
import org.neo4j.register.Register.DoubleLongRegister;

import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class DumpCountsStore
{
    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 1 )
        {
            System.out.println( "one argument describing the path to the store" );
            System.exit( 1 );
        }

        final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        final File storeFile = new File( args[0] );

        try ( final StandalonePageCache pageCache = createPageCache( fs, "counts-store-dump" ) )
        {
            CountsStore counts = CountsStore.open( fs, pageCache, storeFile );
            System.out.println( "Counts Store: " + counts.file() );
            System.out.println( "\ttxId: " + counts.lastTxId() );
            System.out.println( "\tminor version: " + counts.minorVersion() );
            System.out.println( "\tentries: " + counts.totalRecordsStored() );
            System.out.println( "Entries:" );

            counts.accept( new KeyValueRecordVisitor<CountsKey,CopyableDoubleLongRegister>()
            {
                private final DoubleLongRegister tmp = newDoubleLongRegister();

                @Override
                public void visit( CountsKey key, CopyableDoubleLongRegister register )
                {
                    register.copyTo( tmp );
                    System.out.println( "\t" + key + ": (" + tmp.readFirst() + ", " + tmp.readSecond() + ")" );
                }
            }, newDoubleLongRegister() );
        }
    }
}
