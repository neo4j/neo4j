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

import org.apache.commons.lang3.ObjectUtils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.pagecache.AdversarialPageCache;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.checking.AccessCheckingPageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;

public class PageCacheRule extends ExternalResource
{
    /**
     * Class to alter behavior and configuration of {@link PageCache} instances opened in this rule.
     */
    public static final class PageCacheConfig
    {
        protected Boolean inconsistentReads;
        protected Integer pageSize;
        protected AtomicBoolean nextReadIsInconsistent;
        protected PageCacheTracer tracer;
        protected PageCursorTracerSupplier pageCursorTracerSupplier;
        private boolean accessChecks;
        private String memory;

        private PageCacheConfig()
        {
        }

        /**
         * Sets whether or not to decorate PageCache where the read page cursors will randomly produce inconsistent
         * reads with a ~50% probability.
         *
         * @param inconsistentReads {@code true} if PageCache should be decorated with read cursors with
         * randomly inconsistent reads.
         * @return this instance.
         */
        public PageCacheConfig withInconsistentReads( boolean inconsistentReads )
        {
            this.inconsistentReads = inconsistentReads;
            return this;
        }

        /**
         * Decorated PageCache where the next page read from a read page cursor will be
         * inconsistent if the given AtomicBoolean is set to 'true'. The AtomicBoolean is automatically
         * switched to 'false' when the inconsistent read is performed, to prevent code from looping
         * forever.
         *
         * @param nextReadIsInconsistent an {@link AtomicBoolean} for controlling when inconsistent reads happen.
         * @return this instance.
         */
        public PageCacheConfig withInconsistentReads( AtomicBoolean nextReadIsInconsistent )
        {
            this.nextReadIsInconsistent = nextReadIsInconsistent;
            this.inconsistentReads = true;
            return this;
        }

        /**
         * Makes PageCache have the specified page size.
         *
         * @param pageSize page size to use instead of hinted page size.
         * @return this instance.
         */
        public PageCacheConfig withPageSize( int pageSize )
        {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * {@link PageCacheTracer} to use for the PageCache.
         *
         * @param tracer {@link PageCacheTracer} to use.
         * @return this instance.
         */
        public PageCacheConfig withTracer( PageCacheTracer tracer )
        {
            this.tracer = tracer;
            return this;
        }

        /**
         * {@link PageCursorTracerSupplier} to use for this page cache.
         * @param tracerSupplier supplier of page cursors tracers
         * @return this instance
         */
        public PageCacheConfig withCursorTracerSupplier( PageCursorTracerSupplier tracerSupplier )
        {
            this.pageCursorTracerSupplier = tracerSupplier;
            return this;
        }

        /**
         * Decorates PageCache with access checking wrapper to add some amount of verifications that
         * reads happen inside shouldRetry-loops.
         *
         * @param accessChecks whether or not to add access checking to the opened PageCache.
         * @return this instance.
         */
        public PageCacheConfig withAccessChecks( boolean accessChecks )
        {
            this.accessChecks = accessChecks;
            return this;
        }

        /**
         * Overrides default memory setting, which is a standard test size of '8 MiB'.
         *
         * @param memory memory setting to use for this page cache.
         * @return this instance.
         */
        public PageCacheConfig withMemory( String memory )
        {
            this.memory = memory;
            return this;
        }
    }

    /**
     * @return new {@link PageCacheConfig} instance.
     */
    public static PageCacheConfig config()
    {
        return new PageCacheConfig();
    }

    protected PageCache pageCache;
    final PageCacheConfig baseConfig;

    public PageCacheRule()
    {
        this( config() );
    }

    public PageCacheRule( PageCacheConfig config )
    {
        this.baseConfig = config;
    }

    public PageCache getPageCache( FileSystemAbstraction fs )
    {
        return getPageCache( fs, config() );
    }

    /**
     * Opens a new {@link PageCache} with the provided file system and config.
     *
     * @param fs {@link FileSystemAbstraction} to use for the {@link PageCache}.
     * @param overriddenConfig specific {@link PageCacheConfig} overriding config provided in {@link PageCacheRule}
     * constructor, if any.
     * @return the opened {@link PageCache}.
     */
    public PageCache getPageCache( FileSystemAbstraction fs, PageCacheConfig overriddenConfig )
    {
        closeExistingPageCache();
        Integer pageSize = selectConfig( baseConfig.pageSize, overriddenConfig.pageSize, null );
        PageCacheTracer cacheTracer = selectConfig( baseConfig.tracer, overriddenConfig.tracer, PageCacheTracer.NULL );
        PageCursorTracerSupplier cursorTracerSupplier = selectConfig(
                baseConfig.pageCursorTracerSupplier,
                overriddenConfig.pageCursorTracerSupplier,
                PageCursorTracerSupplier.NULL );

        SingleFilePageSwapperFactory factory = new SingleFilePageSwapperFactory();
        factory.open( fs, Configuration.EMPTY );

        VersionContextSupplier contextSupplier = EmptyVersionContextSupplier.EMPTY;
        MemoryAllocator mman = MemoryAllocator.createAllocator( selectConfig( baseConfig.memory, overriddenConfig.memory, "8 MiB" ),
                new LocalMemoryTracker() );
        if ( pageSize != null )
        {
            pageCache = new MuninnPageCache( factory, mman, pageSize, cacheTracer, cursorTracerSupplier,
                    contextSupplier );
        }
        else
        {
            pageCache = new MuninnPageCache( factory, mman, cacheTracer, cursorTracerSupplier, contextSupplier );
        }
        pageCachePostConstruct( overriddenConfig );
        return pageCache;
    }

    protected static <T> T selectConfig( T base, T overridden, T defaultValue )
    {
        return ObjectUtils.firstNonNull( base, overridden, defaultValue );
    }

    protected void pageCachePostConstruct( PageCacheConfig overriddenConfig )
    {
        if ( selectConfig( baseConfig.inconsistentReads, overriddenConfig.inconsistentReads, true ) )
        {
            AtomicBoolean controller = selectConfig( baseConfig.nextReadIsInconsistent,
                    overriddenConfig.nextReadIsInconsistent, null );
            Adversary adversary = controller != null
                    ? new AtomicBooleanInconsistentReadAdversary( controller )
                    : new RandomInconsistentReadAdversary();
            pageCache = new AdversarialPageCache( pageCache, adversary );
        }
        if ( selectConfig( baseConfig.accessChecks, overriddenConfig.accessChecks, false ) )
        {
            pageCache = new AccessCheckingPageCache( pageCache );
        }
    }

    protected void closeExistingPageCache()
    {
        if ( pageCache != null )
        {
            try
            {
                pageCache.close();
            }
            catch ( Exception e )
            {
                throw new AssertionError(
                        "Failed to stop existing PageCache prior to creating a new one", e );
            }
        }
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
            catch ( Exception e )
            {
                throw new AssertionError( "Failed to stop PageCache after test", e );
            }
            pageCache = null;
        }
    }

    public static class AtomicBooleanInconsistentReadAdversary implements Adversary
    {
        final AtomicBoolean nextReadIsInconsistent;

        public AtomicBooleanInconsistentReadAdversary( AtomicBoolean nextReadIsInconsistent )
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
