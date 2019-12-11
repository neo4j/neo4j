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

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

/**
 * Class to alter behavior and configuration of {@link PageCache} instances opened in this rule.
 */
public final class PageCacheConfig
{
    protected Boolean inconsistentReads;
    protected Integer pageSize;
    protected AtomicBoolean nextReadIsInconsistent;
    protected PageCacheTracer tracer;
    protected boolean accessChecks;
    protected String memory;

    /**
     * @return new {@link PageCacheConfig} instance.
     */
    public static PageCacheConfig config()
    {
        return new PageCacheConfig();
    }

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
