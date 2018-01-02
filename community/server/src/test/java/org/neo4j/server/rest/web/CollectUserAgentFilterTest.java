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
package org.neo4j.server.rest.web;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.concurrent.RecentK;
import org.neo4j.test.SuppressOutput;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.test.SuppressOutput.suppressAll;

public class CollectUserAgentFilterTest
{
    private final RecentK<String> agents = new RecentK<>(10);
    private final FilterChain filterChain = mock( FilterChain.class );
    private final CollectUserAgentFilter filter = new CollectUserAgentFilter( agents );

    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    @Test
    public void shouldRecordASingleUserAgent() throws IOException, ServletException
    {
        filter.doFilter( request( "the-agent" ), null, filterChain );
        assertThat( agents.recentItems(), hasItem( "the-agent" ) );
    }

    @Test
    public void shouldOnlyRecordTheFirstFieldOfTheUserAgentString() throws IOException, ServletException
    {
        filter.doFilter( request( "the-agent other-info" ), null, filterChain );
        assertThat( agents.recentItems(), hasItem( "the-agent" ) );
    }

    @Test
    public void shouldRecordMultipleUserAgents() throws IOException, ServletException
    {
        filter.doFilter( request( "agent1" ), null, filterChain );
        filter.doFilter( request( "agent2" ), null, filterChain );
        assertThat( agents.recentItems(), hasItems( "agent1", "agent2" ) );
    }

    @Test
    public void shouldNotReportDuplicates() throws IOException, ServletException
    {
        filter.doFilter( request( "the-agent" ), null, filterChain );
        filter.doFilter( request( "the-agent" ), null, filterChain );
        assertThat( agents.recentItems(), hasSize( 1 ) );
    }

    @Test
    public void shouldCopeIfThereIsNoUserAgentHeader() throws IOException, ServletException
    {
        filter.doFilter( request( null ), null, filterChain );
        assertThat( agents.recentItems(), hasSize( 0 ) );
    }

    @Test
    public void shouldSwallowAnyExceptionsThrownByTheRequest() throws IOException, ServletException
    {
        HttpServletRequest request = mock( HttpServletRequest.class );
        stub(request.getHeader( anyString() )).toThrow( new RuntimeException() );
        filter.doFilter( request, null, filterChain );
    }

    @Test
    public void shouldReturnTheRequest() throws IOException, ServletException
    {
        ServletRequest original = request( "the-agent" );
        filter.doFilter( original, null, filterChain );
        verify(filterChain).doFilter( original, null );
    }

    private static ServletRequest request( String userAgent )
    {
        HttpServletRequest request = mock( HttpServletRequest.class );
        when( request.getHeader( "User-Agent" ) ).thenReturn( userAgent );
        return request;
    }
}
