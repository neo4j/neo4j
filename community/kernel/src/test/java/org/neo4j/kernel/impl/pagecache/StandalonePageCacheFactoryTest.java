/*
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
package org.neo4j.kernel.impl.pagecache;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.fs.DelegateFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.junit.Assert.assertTrue;

public class StandalonePageCacheFactoryTest
{
    @Test( timeout = 10000 )
    public void mustAutomaticallyStartEvictionThread() throws IOException
    {
        FileSystemAbstraction fs = new DelegateFileSystemAbstraction( Jimfs.newFileSystem( jimConfig() ) );
        File file = new File( "/a" ).getCanonicalFile();
        fs.create( file ).close();

        try ( PageCache cache = StandalonePageCacheFactory.createPageCache( fs );
              PagedFile pf = cache.map( file, 4096 );
              PageCursor cursor = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            // The default size is currently 8MBs.
            // It should be possible to write more than that.
            // If the eviction thread has not been started, then this test will block forever.
            for ( int i = 0; i < 10_000; i++ )
            {
                assertTrue( cursor.next() );
                cursor.putInt( 42 );
            }
        }
    }

    private Configuration jimConfig()
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            List<String> rootList = new ArrayList<>();
            FileSystems.getDefault().getRootDirectories().forEach( path -> rootList.add( path.toString() ) );
            Configuration.Builder builder = Configuration.windows().toBuilder();
            if ( rootList.size() > 1 )
            {
                builder.setRoots( rootList.get( 0 ), rootList.subList( 1, rootList.size() ).toArray(new String[0] ) );
            }
            else
            {
                builder.setRoots( rootList.get( 0 ) );
            }
        }
        else if ( SystemUtils.IS_OS_MAC_OSX )
        {
            return Configuration.osX();
        }
        return Configuration.unix();
    }
}
