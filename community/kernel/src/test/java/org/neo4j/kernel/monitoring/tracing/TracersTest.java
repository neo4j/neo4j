/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class TracersTest
{
    private StringLogger msgLog;
    private StringBuffer logBuffer;

    @Before
    public void setUp()
    {
        logBuffer = new StringBuffer();
        msgLog = StringLogger.wrap( logBuffer );
        System.setProperty( "org.neo4j.helpers.Service.printServiceLoaderStackTraces", "true" );
    }

    @Test
    public void mustProduceNullImplementationsWhenRequested() throws Exception
    {
        Tracers tracers = new Tracers( "null", msgLog );
        assertThat( tracers.pageCacheTracer, is( PageCacheTracer.NULL ) );
        assertThat( tracers.transactionTracer, is( TransactionTracer.NULL ) );
        assertNoWarning();
    }

    @Test
    public void mustProduceNullImplementationsWhenRequestedIgnoringCase() throws Exception
    {
        Tracers tracers = new Tracers( "NuLl", msgLog );
        assertThat( tracers.pageCacheTracer, is( PageCacheTracer.NULL ) );
        assertThat( tracers.transactionTracer, is( TransactionTracer.NULL ) );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationForNullConfiguration() throws Exception
    {
        Tracers tracers = new Tracers( null, msgLog );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationWhenRequested() throws Exception
    {
        Tracers tracers = new Tracers( "default", msgLog );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationWhenRequestedIgnoringCase() throws Exception
    {
        Tracers tracers = new Tracers( "DeFaUlT", msgLog );
        assertDefaultImplementation( tracers );
        assertNoWarning();
    }

    @Test
    public void mustProduceDefaultImplementationWhenRequestingUnknownImplementation() throws Exception
    {
        Tracers tracers = new Tracers( "there's nothing like this", msgLog );
        assertDefaultImplementation( tracers );
        assertWarning();
    }

    private void assertDefaultImplementation( Tracers tracers )
    {
        assertThat( tracers.pageCacheTracer, instanceOf( DefaultPageCacheTracer.class ) );
        assertThat( tracers.transactionTracer, is( TransactionTracer.NULL ) );
    }

    private void assertNoWarning()
    {
        assertThat( logBuffer.toString(), is( "" ) );
    }

    private void assertWarning()
    {
        assertThat( logBuffer.toString(), not( is( "" ) ) );
    }
}
