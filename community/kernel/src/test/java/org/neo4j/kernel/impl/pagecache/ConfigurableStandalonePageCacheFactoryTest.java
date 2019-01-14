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
package org.neo4j.kernel.impl.pagecache;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.VerboseTimeout;

import static org.junit.Assert.assertTrue;

public class ConfigurableStandalonePageCacheFactoryTest
{
    @Rule
    public VerboseTimeout timeout = VerboseTimeout.builder().withTimeout( 30, TimeUnit.SECONDS ).build();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void mustAutomaticallyStartEvictionThread() throws IOException
    {
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction() )
        {
            File file = new File( testDirectory.directory(), "a" ).getCanonicalFile();
            fs.create( file ).close();

            try ( PageCache cache = ConfigurableStandalonePageCacheFactory.createPageCache( fs );
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
    }
}
