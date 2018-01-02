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
package org.neo4j.server.rest.security;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;

public class SecurityFilterTest
{
    @Test
    public void shouldPassThroughRequestToAnUnsecuredPath() throws Exception
    {
        // given
        SecurityRule rule = mock( SecurityRule.class );
        when( rule.forUriPath() ).thenReturn( "/some-path" );

        FilterChain filterChain = mock( FilterChain.class );

        SecurityFilter securityFilter = new SecurityFilter( rule );

        HttpServletRequest request = mock( HttpServletRequest.class );
        when( request.getContextPath() ).thenReturn( "/some-other-path" );

        // when
        securityFilter.doFilter( request, mock( HttpServletResponse.class ), filterChain );

        // then
        verify( filterChain ).doFilter( any( HttpServletRequest.class ), any( HttpServletResponse.class ) );
    }

    @Test
    public void shouldActivateRuleThatRejectsTheRequestForAMatchingPath() throws Exception
    {
        // given
        SecurityRule rule = mock( SecurityRule.class );
        when( rule.forUriPath() ).thenReturn( "/some-path" );
        when( rule.isAuthorized( any( HttpServletRequest.class ) ) ).thenReturn( false );

        FilterChain filterChain = mock( FilterChain.class );

        SecurityFilter securityFilter = new SecurityFilter( rule );

        HttpServletRequest request = mock( HttpServletRequest.class );
        when( request.getContextPath() ).thenReturn( "/some-path" );

        // when
        securityFilter.doFilter( request, mock( HttpServletResponse.class ), filterChain );

        // then
        verify( filterChain, times(0) ).doFilter( any( HttpServletRequest.class ), any( HttpServletResponse.class ) );
    }


    @Test
    public void shouldActivateRuleThatAcceptsTheRequestForAMatchingPath() throws Exception
    {
        // given
        SecurityRule rule = mock( SecurityRule.class );
        when( rule.forUriPath() ).thenReturn( "/some-path" );
        when( rule.isAuthorized( any( HttpServletRequest.class ) ) ).thenReturn( true );

        FilterChain filterChain = mock( FilterChain.class );

        SecurityFilter securityFilter = new SecurityFilter( rule );

        HttpServletRequest request = mock( HttpServletRequest.class );
        when( request.getContextPath() ).thenReturn( "/some-path" );

        HttpServletResponse response = mock( HttpServletResponse.class );

        // when
        securityFilter.doFilter( request, response, filterChain );

        // then
        verify( filterChain ).doFilter( request, response );
    }

    @Test
    public void shouldRemoveRules() throws Exception
    {
        // given
        SecurityRule securityRule1 = mock( SecurityRule.class );
        when( securityRule1.forUriPath() ).thenReturn( "/securityRule1" );

        SecurityRule securityRule2 = mock( SecurityRule.class );
        when( securityRule2.forUriPath() ).thenReturn( "/securityRule2" );

        SecurityFilter securityFilter = new SecurityFilter( securityRule1, securityRule2 );


        HttpServletRequest request = mock( HttpServletRequest.class );
        HttpServletResponse response = mock( HttpServletResponse.class );
        FilterChain filterChain = mock( FilterChain.class );

        // when
        securityFilter.destroy();
        securityFilter.doFilter( request, response, filterChain );

        // then
        verify( filterChain ).doFilter( request, response );
    }
}
