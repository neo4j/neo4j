/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SecurityFilter implements Filter
{

    private final SecurityRule rule;
	private final UriPathWildcardMatcher pathMatcher;

    public SecurityFilter( SecurityRule rule )
    {
        this.rule = rule;
        
        // For backwards compatibility
        String rulePath = rule.forUriPath();
        if(!rulePath.endsWith("*"))
        {
        	rulePath = rulePath + "*";
        }
        
        this.pathMatcher = new UriPathWildcardMatcher( rulePath );
    }
    
    public SecurityRule getRule() 
    {
    	return rule;
    }

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {
    }

    @Override
    public void doFilter( ServletRequest request, ServletResponse response, FilterChain chain ) throws IOException,
            ServletException
    {

        validateRequestType( request );
        validateResponseType( response );
        
        HttpServletRequest httpReq = (HttpServletRequest) request;
        String path = httpReq.getContextPath() + (httpReq.getPathInfo() == null ? "" : httpReq.getPathInfo());
        
        pathMatcher.matches(path);
        
        if ( !pathMatcher.matches(path) || rule.isAuthorized( httpReq ) )
        {
            chain.doFilter( request, response );
        }
        else
        {
            createUnauthorizedChallenge( response );
        }
    }

    private void validateRequestType( ServletRequest request ) throws ServletException
    {
        if ( !( request instanceof HttpServletRequest ) )
        {
            throw new ServletException( String.format( "Expected HttpServletRequest, received [%s]", request.getClass()
                    .getCanonicalName() ) );
        }
    }

    private void validateResponseType( ServletResponse response ) throws ServletException
    {
        if ( !( response instanceof HttpServletResponse ) )
        {
            throw new ServletException( String.format( "Expected HttpServletResponse, received [%s]",
                    response.getClass()
                            .getCanonicalName() ) );
        }
    }

    private void createUnauthorizedChallenge( ServletResponse response )
    {
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        httpServletResponse.setStatus( 401 );
        httpServletResponse.addHeader( "WWW-Authenticate", rule.wwwAuthenticateHeader() );
    }

    @Override
    public void destroy()
    {
    }

    public static String basicAuthenticationResponse( String realm )
    {
        return "Basic realm=\"" + realm + "\"";
    }
}
