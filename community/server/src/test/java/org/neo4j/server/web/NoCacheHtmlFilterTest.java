package org.neo4j.server.web;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class NoCacheHtmlFilterTest
{
    @Test
    public void shouldAddCacheControlHeaderToHtmlResponses() throws Exception
    {
        // given
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServletPath()).thenReturn( "index.html" );
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock( FilterChain.class );

        // when
        new NoCacheHtmlFilter().doFilter( request, response, filterChain );

        // then
        verify( response ).addHeader( "Cache-Control", "no-cache" );
        verify( filterChain ).doFilter( request, response );
    }

    @Test
    public void shouldPassThroughRequestsForNotHtmlResources() throws Exception
    {
        // given
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServletPath()).thenReturn( "index.js" );
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock( FilterChain.class );

        // when
        new NoCacheHtmlFilter().doFilter( request, response, filterChain );

        // then
        verifyZeroInteractions( response );
        verify( filterChain ).doFilter( request, response );
    }

    @Test
    public void shouldPassThroughRequestsWithNullServletPath() throws Exception
    {
        // given
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServletPath()).thenReturn( null );
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock( FilterChain.class );

        // when
        new NoCacheHtmlFilter().doFilter( request, response, filterChain );

        // then
        verifyZeroInteractions( response );
        verify( filterChain ).doFilter( request, response );
    }
}