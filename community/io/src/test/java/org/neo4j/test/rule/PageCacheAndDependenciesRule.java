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
package org.neo4j.test.rule;

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.neo4j.test.rule.PageCacheRule.config;

/**
 * Very often when you want a {@link PageCacheRule} you also want {@link TestDirectory} and some {@link FileSystemRule}.
 * This is tedious to write and apply in the correct order in every test doing this. This rule collects
 * this threesome into one rule for convenience.
 */
public class PageCacheAndDependenciesRule implements TestRule
{
    private RuleChain chain;
    private FileSystemRule<? extends FileSystemAbstraction> fs;
    private TestDirectory directory;
    private PageCacheRule pageCacheRule;
    private PageCacheRule.PageCacheConfig pageCacheConfig = config();
    private Class<?> clazz;

    public PageCacheAndDependenciesRule with( FileSystemRule<? extends FileSystemAbstraction> fs )
    {
        this.fs = fs;
        return this;
    }

    public PageCacheAndDependenciesRule with( PageCacheRule.PageCacheConfig config )
    {
        this.pageCacheConfig = config;
        return this;
    }

    public PageCacheAndDependenciesRule with( Class<?> clazz )
    {
        this.clazz = clazz;
        return this;
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        if ( chain == null )
        {
            if ( fs == null )
            {
                fs = new EphemeralFileSystemRule();
            }
            this.pageCacheRule = new PageCacheRule( pageCacheConfig );
            this.directory = TestDirectory.testDirectory( clazz, fs );
            this.chain = RuleChain.outerRule( fs ).around( directory ).around( pageCacheRule );
        }

        return chain.apply( base, description );
    }

    public FileSystemRule<? extends FileSystemAbstraction> fileSystemRule()
    {
        return fs;
    }

    public FileSystemAbstraction fileSystem()
    {
        return fs.get();
    }

    public TestDirectory directory()
    {
        return directory;
    }

    public PageCacheRule pageCacheRule()
    {
        return pageCacheRule;
    }

    public PageCache pageCache()
    {
        return pageCacheRule.getPageCache( fs );
    }
}
