/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.checking.AccessCheckingPageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.rule.RandomInconsistentReadAdversary;

import static org.neo4j.io.ByteUnit.kibiBytes;

/**
 * Simple test utility to create {@link MuninnPageCache} with different settings and {@link Adversary adversaries}.
 */
class PageCacheTestUtil
{
    static class Builder
    {
        private final FileSystemAbstraction fs;
        private PageSwapperFactory swapperFactory;
        private int maxPages = 10_000;
        private int pageSize = (int) kibiBytes( 1 );
        private PageCacheTracer tracer = PageCacheTracer.NULL;
        private Adversary adversary;
        private boolean checkAccess;

        Builder( FileSystemAbstraction fs )
        {
            this.fs = fs;
        }

        Builder swapperFactory( PageSwapperFactory swapperFactory )
        {
            this.swapperFactory = swapperFactory;
            return this;
        }

        Builder maxPages( int maxPages )
        {
            this.maxPages = maxPages;
            return this;
        }

        Builder pageSize( int pageSize )
        {
            this.pageSize = pageSize;
            return this;
        }

        Builder tracer( PageCacheTracer tracer )
        {
            this.tracer = tracer;
            return this;
        }

        Builder adversary( Adversary adversary )
        {
            this.adversary = adversary;
            return this;
        }

        Builder inconsistentReadAdversary()
        {
            return adversary( new RandomInconsistentReadAdversary() );
        }

        Builder checkAccess()
        {
            this.checkAccess = true;
            return this;
        }

        PageCache build()
        {
            PageSwapperFactory swapperFactory =
                    this.swapperFactory != null ? this.swapperFactory : defaultSwapperFactory( fs );
            PageCache pageCache = new MuninnPageCache( swapperFactory, maxPages, pageSize, tracer );

            if ( adversary != null )
            {
                pageCache = new AdversarialPageCache( pageCache, adversary );
            }
            if ( checkAccess )
            {
                pageCache = new AccessCheckingPageCache( pageCache );
            }
            return pageCache;
        }
    }

    private static PageSwapperFactory defaultSwapperFactory( FileSystemAbstraction fs )
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( fs );
        return swapperFactory;
    }

    static Builder pageCache( FileSystemAbstraction fs )
    {
        return new Builder( fs );
    }
}
