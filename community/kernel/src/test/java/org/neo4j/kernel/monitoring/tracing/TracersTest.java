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
package org.neo4j.kernel.monitoring.tracing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.impl.api.DefaultTransactionTracer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DefaultCheckPointerTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

class TracersTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final SystemNanoClock clock = Clocks.nanoClock();
    private final Monitors monitors = new Monitors();

    private Log log;

    @BeforeEach
    void setUp()
    {
        log = logProvider.getLog( getClass() );
        System.setProperty( "org.neo4j.helpers.Service.printServiceLoaderStackTraces", "true" );
    }

    @Test
    void mustProduceNullImplementationsWhenRequested()
    {
        Tracers tracers = createTracers( "null" );
        assertThat( tracers.getPageCacheTracer(), is( PageCacheTracer.NULL ) );
        assertThat( tracers.getPageCursorTracerSupplier(), is( PageCursorTracerSupplier.NULL ) );
        assertThat( tracers.getTransactionTracer(), is( TransactionTracer.NULL ) );
        assertNoWarning();
    }

    @Test
    void mustProduceDefaultImplementationForNullConfiguration()
    {
        Tracers tracers = createTracers( null );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    void mustProduceDefaultImplementationWhenRequested()
    {
        Tracers tracers = createTracers( "default" );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    void mustProduceDefaultImplementationWhenRequestingUnknownImplementation()
    {
        Tracers tracers = createTracers( "there's nothing like this" );
        assertDefaultImplementation( tracers );
        assertWarning( "there's nothing like this" );
    }

    private Tracers createTracers( String s )
    {
        return new Tracers( s, log, monitors, jobScheduler, clock );
    }

    private void assertDefaultImplementation( Tracers tracers )
    {
        assertThat( tracers.getPageCacheTracer(), instanceOf( DefaultPageCacheTracer.class ) );
        assertThat( tracers.getTransactionTracer(), instanceOf( DefaultTransactionTracer.class ) );
        assertThat( tracers.getCheckPointTracer(), instanceOf( DefaultCheckPointerTracer.class ) );
        assertThat( tracers.getPageCursorTracerSupplier(), instanceOf( DefaultPageCursorTracerSupplier.class ) );
    }

    private void assertNoWarning()
    {
        logProvider.assertNoLoggingOccurred();
    }

    private void assertWarning( String implementationName )
    {
        logProvider.assertExactly(
                AssertableLogProvider.inLog( getClass() ).warn( "Using default tracer implementations instead of '%s'", implementationName )
        );
    }
}
