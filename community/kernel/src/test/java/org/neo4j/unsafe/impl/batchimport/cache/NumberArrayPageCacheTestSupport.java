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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.test.rule.TestDirectory;

public class NumberArrayPageCacheTestSupport
{
    public static Fixture prepareDirectoryAndPageCache( Class<?> testClass ) throws IOException
    {
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        TestDirectory testDirectory = TestDirectory.testDirectory( testClass, fileSystem );
        File dir = testDirectory.prepareDirectoryForTest( "test" );
        PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem );
        return new Fixture( pageCache, fileSystem, dir );
    }

    public static class Fixture implements AutoCloseable
    {
        public final PageCache pageCache;
        public final FileSystemAbstraction fileSystem;
        public final File directory;

        private Fixture( PageCache pageCache, FileSystemAbstraction fileSystem, File directory )
        {
            this.pageCache = pageCache;
            this.fileSystem = fileSystem;
            this.directory = directory;
        }

        @Override
        public void close() throws Exception
        {
            pageCache.close();
            fileSystem.close();
        }
    }
}
