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
package org.neo4j.index;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.index.schema.NativeIndexHeaderWriter;
import org.neo4j.kernel.impl.index.schema.NativeIndexes;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;

public class SetInitialStateInNativeIndex extends NativeIndexRestartAction
{
    private final byte targetInitialState;

    public SetInitialStateInNativeIndex( byte targetInitialState, IndexProviderDescriptor providerDescriptor )
    {
        super( providerDescriptor );
        this.targetInitialState = targetInitialState;
    }

    @Override
    protected void runOnDirectoryStructure( FileSystemAbstraction fs, IndexDirectoryStructure indexDirectoryStructure ) throws IOException
    {
        PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs, JobSchedulerFactory.createInitialisedScheduler() );
        int filesChanged = setInitialState( fs, indexDirectoryStructure.rootDirectory(), pageCache );
        assertThat( "couldn't find any index to set state on", filesChanged, greaterThanOrEqualTo( 1 ) );
    }

    private int setInitialState( FileSystemAbstraction fs, File fileOrDir, PageCache pageCache ) throws IOException
    {
        if ( fs.isDirectory( fileOrDir ) )
        {
            int count = 0;
            File[] children = fs.listFiles( fileOrDir );
            if ( children != null )
            {
                for ( File child : children )
                {
                    count += setInitialState( fs, child, pageCache );
                }
            }
            return count;
        }
        else
        {
            if ( isNativeIndexFile( fileOrDir, pageCache ) )
            {
                overwriteState( pageCache, fileOrDir, targetInitialState );
            }
            return 1;
        }
    }

    private boolean isNativeIndexFile( File fileOrDir, PageCache pageCache )
    {
        try
        {
            // Try and read initial state, if we succeed then we know this file is a native index file
            // and we overwrite initial state with target.
            NativeIndexes.readState( pageCache, fileOrDir );
            return true;
        }
        catch ( Throwable t )
        {
            return false;
        }
    }

    private static void overwriteState( PageCache pageCache, File indexFile, byte state ) throws IOException
    {
        NativeIndexHeaderWriter stateWriter = new NativeIndexHeaderWriter( state, NO_HEADER_WRITER );
        GBPTree.overwriteHeader( pageCache, indexFile, stateWriter );
    }
}
