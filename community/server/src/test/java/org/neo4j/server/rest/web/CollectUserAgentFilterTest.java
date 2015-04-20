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
package org.neo4j.server.rest.web;

import java.util.Arrays;
import java.util.List;

import com.sun.jersey.spi.container.ContainerRequest;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.Mute;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

import static org.neo4j.test.Mute.muteAll;

public class CollectUserAgentFilterTest
{
    private final CollectUserAgentFilter filter = new CollectUserAgentFilter();

    @Rule
    public Mute mute = muteAll();

    @Test
    public void shouldRecordASingleUserAgent()
    {
        filter.filter( request( "the-agent" ) );
        assertThat( filter.getUserAgents(), hasItem( "the-agent" ) );
    }

    @Test
    public void shouldOnlyRecordTheFirstFieldOfTheUserAgentString()
    {
        filter.filter( request( "the-agent other-info" ) );
        assertThat( filter.getUserAgents(), hasItem( "the-agent" ) );
    }

    @Test
    public void shouldRecordMultipleUserAgents()
    {
        filter.filter( request( "agent1" ) );
        filter.filter( request( "agent2" ) );
        assertThat( filter.getUserAgents(), hasItems( "agent1", "agent2" ) );
    }

    @Test
    public void shouldNotReportDuplicates()
    {
        filter.filter( request( "the-agent" ) );
        filter.filter( request( "the-agent" ) );
        assertThat( filter.getUserAgents(), hasSize( 1 ) );
    }

    @Test
    public void shouldClearRecordedValues()
    {
        filter.filter( request( "the-agent" ) );
        filter.reset();
        assertThat( filter.getUserAgents(), hasSize( 0 ) );
    }

    @Test
    public void shouldCopeIfThereIsNoUserAgentHeader()
    {
        filter.filter( request() );
        assertThat( filter.getUserAgents(), hasSize( 0 ) );
    }

    @Test
    public void shouldCopeIfThereIsMoreThanOneUserAgentHeader()
    {
        filter.filter( request( "agent1", "agent2" ) );
        assertThat( filter.getUserAgents(), hasSize( 1 ) );
    }

    @Test
    public void shouldSwallowAnyExceptionsThrownByTheRequest()
    {
        ContainerRequest request = mock( ContainerRequest.class );
        stub(request.getRequestHeader( anyString() )).toThrow( new RuntimeException() );
        filter.filter( request );
    }

    @Test
    public void shouldReturnTheRequest()
    {
        ContainerRequest original = request( "the-agent" );
        ContainerRequest returned = filter.filter( original );
        assertSame( original, returned );
    }

    private static ContainerRequest request( String... userAgent )
    {
        ContainerRequest request = mock( ContainerRequest.class );
        List<String> headers = Arrays.asList( userAgent );
        stub(request.getRequestHeader( "User-Agent" )).toReturn( headers );
        return request;
    }
}
