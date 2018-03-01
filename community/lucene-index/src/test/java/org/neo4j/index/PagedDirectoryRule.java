/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index;

import org.apache.lucene.store.Directory;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.test.rule.ExternalResource;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class PagedDirectoryRule extends ExternalResource implements DirectoryFactory
{
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    public PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule );

    private DirectoryFactory dir;

    private final FileSystemAbstraction overridingFileSystem;

    public PagedDirectoryRule()
    {
        this( null );
    }

    public PagedDirectoryRule( FileSystemAbstraction overridingFileSystem )
    {
        this.overridingFileSystem = overridingFileSystem;
    }

    @Override
    protected void before()
    {
        this.dir = new PagedDirectoryFactory(
                pageCacheRule.getPageCache( overridingFileSystem != null ? overridingFileSystem : fileSystemRule ) );
    }

    @Override
    protected void after( boolean successful )
    {
        dir.close();
    }

    @Override
    public Directory open( File f ) throws IOException
    {
        return dir.open( f );
    }

    @Override
    public void close()
    {
        // no-op
    }
}
