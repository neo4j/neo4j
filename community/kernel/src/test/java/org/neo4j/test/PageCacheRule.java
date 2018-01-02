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
package org.neo4j.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;

public class PageCacheRule extends ExternalResource
{
    private PageCache pageCache;
    private final boolean automaticallyProduceInconsistentReads;

    public PageCacheRule()
    {
        automaticallyProduceInconsistentReads = true;
    }

    public PageCacheRule( boolean automaticallyProduceInconsistentReads )
    {
        this.automaticallyProduceInconsistentReads = automaticallyProduceInconsistentReads;
    }

    public PageCache getPageCache( FileSystemAbstraction fs )
    {
        Map<String,String> settings = new HashMap<>();
        settings.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        return getPageCache( fs, new Config( settings ) );
    }

    public PageCache getPageCache( FileSystemAbstraction fs, Config config )
    {
        if ( pageCache != null )
        {
            try
            {
                pageCache.close();
            }
            catch ( IOException e )
            {
                throw new AssertionError(
                        "Failed to stop existing PageCache prior to creating a new one", e );
            }
        }

        pageCache = StandalonePageCacheFactory.createPageCache( fs, config );

        if ( automaticallyProduceInconsistentReads )
        {
            return withInconsistentReads( pageCache );
        }
        return pageCache;
    }

    @Override
    protected void after( boolean success )
    {
        if ( pageCache != null )
        {
            try
            {
                pageCache.close();
            }
            catch ( IOException e )
            {
                throw new AssertionError( "Failed to stop PageCache after test", e );
            }
            pageCache = null;
        }
    }

    /**
     * Returns a decorated PageCache where the next page read from a read page cursor will be
     * inconsistent if the given AtomicBoolean is set to 'true'. The AtomicBoolean is automatically
     * switched to 'false' when the inconsistent read is performed, to prevent code from looping
     * forever.
     */
    public PageCache withInconsistentReads( PageCache pageCache, AtomicBoolean nextReadIsInconsistent )
    {
        Adversary adversary = new AtomicBooleanInconsistentReadAdversary( nextReadIsInconsistent );
        return new AdversarialPageCache( pageCache, adversary );
    }

    /**
     * Returns a decorated PageCache where the read page cursors will randomly produce inconsistent
     * reads with a ~50% probability.
     */
    public PageCache withInconsistentReads( PageCache pageCache )
    {
        Adversary adversary = new RandomInconsistentReadAdversary();
        return new AdversarialPageCache( pageCache, adversary );
    }

    private static class AtomicBooleanInconsistentReadAdversary implements Adversary
    {
        final AtomicBoolean nextReadIsInconsistent;

        AtomicBooleanInconsistentReadAdversary( AtomicBoolean nextReadIsInconsistent )
        {
            this.nextReadIsInconsistent = nextReadIsInconsistent;
        }

        @Override
        @SafeVarargs
        public final void injectFailure( Class<? extends Throwable>... failureTypes )
        {
        }

        @Override
        @SafeVarargs
        public final boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
        {
            return nextReadIsInconsistent.getAndSet( false );
        }
    }

    private static class RandomInconsistentReadAdversary implements Adversary
    {
        @Override
        @SafeVarargs
        public final void injectFailure( Class<? extends Throwable>... failureTypes )
        {
        }

        @Override
        @SafeVarargs
        public final boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
        {
            return ThreadLocalRandom.current().nextBoolean();
        }
    }
}
