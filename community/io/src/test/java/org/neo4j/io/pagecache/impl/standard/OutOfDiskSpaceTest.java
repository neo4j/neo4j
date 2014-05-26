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
package org.neo4j.io.pagecache.impl.standard;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.mockfs.LimitedFilesystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.TargetDirectory;

import static junit.framework.TestCase.fail;

public class OutOfDiskSpaceTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test(timeout = 5000)
    public void shouldHandleOutOfDiskSpaceOnBackgroundFlush() throws Exception
    {
        // Given
        LimitedFilesystemAbstraction fs = new LimitedFilesystemAbstraction( new DefaultFileSystemAbstraction() );
        StandardPageCache cache = new StandardPageCache( fs, 2, 512 );

        PagedFile file = cache.map( new File( testDir.directory(), "storefile" ), 512 );
        PageCursor cursor = cache.newCursor();

        // And given the eviction thread is running
        Thread sweeperThread = new Thread( cache );
        sweeperThread.start();

        // And given we've "changed" some pages
        file.pin( cursor, PageLock.EXCLUSIVE, 1 );
        file.unpin( cursor );
        file.pin( cursor, PageLock.EXCLUSIVE, 2 );
        file.unpin( cursor );
        file.pin( cursor, PageLock.EXCLUSIVE, 3 );
        file.unpin( cursor );

        // When
        fs.runOutOfDiskSpace();

        // Then
        // 1: Explicit flushing should throw IO exception
        try
        {
            file.flush();
            fail("Should've thrown out of disk.");
        }
        catch(IOException e)
        {
            // ok
        }

        // 2: The background eviction thread should give up and shut down
        while(sweeperThread.isAlive())
        {
            Thread.sleep( 10 );
            file.pin( cursor, PageLock.EXCLUSIVE, 1 );
            file.unpin( cursor );
        }
    }

}
