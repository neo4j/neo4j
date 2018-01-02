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
package org.neo4j.kernel.monitoring.tracing;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.DefaultTransactionTracer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DefaultCheckPointerTracer;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.logging.Log;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TracersTest
{
    private AssertableLogProvider logProvider;
    private Log log;

    @Before
    public void setUp()
    {
        logProvider = new AssertableLogProvider();
        log = logProvider.getLog( getClass() );
        System.setProperty( "org.neo4j.helpers.Service.printServiceLoaderStackTraces", "true" );
    }

    @Test
    public void mustProduceNullImplementationsWhenRequested() throws Exception
    {
        Tracers tracers = new Tracers( "null", log );
        assertThat( tracers.pageCacheTracer, is( PageCacheTracer.NULL ) );
        assertThat( tracers.transactionTracer, is( TransactionTracer.NULL ) );
        assertNoWarning();
    }

    @Test
    public void mustProduceNullImplementationsWhenRequestedIgnoringCase() throws Exception
    {
        Tracers tracers = new Tracers( "NuLl", log );
        assertThat( tracers.pageCacheTracer, is( PageCacheTracer.NULL ) );
        assertThat( tracers.transactionTracer, is( TransactionTracer.NULL ) );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationForNullConfiguration() throws Exception
    {
        Tracers tracers = new Tracers( null, log );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationWhenRequested() throws Exception
    {
        Tracers tracers = new Tracers( "default", log );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationWhenRequestedIgnoringCase() throws Exception
    {
        Tracers tracers = new Tracers( "DeFaUlT", log );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationWhenRequestingUnknownImplementation() throws Exception
    {
        Tracers tracers = new Tracers( "there's nothing like this", log );
        assertDefaultImplementation( tracers );
        assertWarning( "there's nothing like this" );
    }

    private void assertDefaultImplementation( Tracers tracers )
    {
        assertThat( tracers.pageCacheTracer, instanceOf( DefaultPageCacheTracer.class ) );
        assertThat( tracers.transactionTracer, instanceOf( DefaultTransactionTracer.class ) );
        assertThat( tracers.checkPointTracer, instanceOf( DefaultCheckPointerTracer.class ) );
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
