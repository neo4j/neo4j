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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

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
    private final HashMap<UriPathWildcardMatcher, HashSet<ForbiddingSecurityRule>> rules = new HashMap<UriPathWildcardMatcher, HashSet<ForbiddingSecurityRule>>();

    public SecurityFilter( SecurityRule rule, SecurityRule... rules )
    {
        this( merge( rule, rules ) );
    }

    public SecurityFilter( Iterable<SecurityRule> securityRules )
    {
        // For backwards compatibility
        for ( SecurityRule r : securityRules )
        {
            String rulePath = r.forUriPath();
            if ( !rulePath.endsWith( "*" ) )
            {
                rulePath = rulePath + "*";
            }

            UriPathWildcardMatcher uriPathWildcardMatcher = new UriPathWildcardMatcher( rulePath );
            HashSet<ForbiddingSecurityRule> ruleHashSet = rules.get( uriPathWildcardMatcher );
            if ( ruleHashSet == null )
            {
                ruleHashSet = new HashSet<ForbiddingSecurityRule>();
                rules.put( uriPathWildcardMatcher, ruleHashSet );
            }
            ruleHashSet.add( fromSecurityRule(r) );
        }
    }

    private static ForbiddingSecurityRule fromSecurityRule(final SecurityRule rule) {
        if (rule instanceof ForbiddingSecurityRule)
        {
            return (ForbiddingSecurityRule) rule;
        }
        return new ForbiddenRuleDecorator(rule);
    }

    private static Iterable<SecurityRule> merge( SecurityRule rule, SecurityRule[] rules )
    {
        ArrayList<SecurityRule> result = new ArrayList<SecurityRule>();

        result.add( rule );

        Collections.addAll( result, rules );

        return result;
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

        boolean requestIsForbidden = false;
        for ( UriPathWildcardMatcher uriPathWildcardMatcher : rules.keySet() )
        {
            if ( uriPathWildcardMatcher.matches( path ) )
            {
                HashSet<ForbiddingSecurityRule> securityRules = rules.get( uriPathWildcardMatcher );
                for ( ForbiddingSecurityRule securityRule : securityRules )
                {
                    // 401 on the first failed rule we come along
                    if ( !securityRule.isAuthorized( httpReq ) )
                    {
                        createUnauthorizedChallenge( response, securityRule );
                        return;
                    }
                    requestIsForbidden |= securityRule.isForbidden(httpReq);
                }
            }
        }
        if (requestIsForbidden) {
            createForbiddenResponse( response );
            return;
        }

        chain.doFilter( request, response );
    }


    private void validateRequestType( ServletRequest request ) throws ServletException
    {
        if ( !(request instanceof HttpServletRequest) )
        {
            throw new ServletException( String.format( "Expected HttpServletRequest, received [%s]", request.getClass()
                    .getCanonicalName() ) );
        }
    }

    private void validateResponseType( ServletResponse response ) throws ServletException
    {
        if ( !(response instanceof HttpServletResponse) )
        {
            throw new ServletException( String.format( "Expected HttpServletResponse, received [%s]",
                    response.getClass()
                            .getCanonicalName() ) );
        }
    }

    private void createUnauthorizedChallenge( ServletResponse response, SecurityRule rule )
    {
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        httpServletResponse.setStatus( 401 );
        httpServletResponse.addHeader( "WWW-Authenticate", rule.wwwAuthenticateHeader() );
    }

    private void createForbiddenResponse( ServletResponse response )
    {
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        httpServletResponse.setStatus(403);
    }


    @Override
    public synchronized void destroy()
    {
        rules.clear();
    }

    public static String basicAuthenticationResponse( String realm )
    {
        return "Basic realm=\"" + realm + "\"";
    }

    private static class ForbiddenRuleDecorator implements ForbiddingSecurityRule {
        private final SecurityRule innerRule;

        public ForbiddenRuleDecorator(SecurityRule rule) {
            this.innerRule = rule;
        }

        @Override
        public boolean isForbidden(HttpServletRequest request) {
            return false;
        }

        @Override
        public boolean isAuthorized(HttpServletRequest request) {
            return innerRule.isAuthorized(request);
        }

        @Override
        public String forUriPath() {
            return innerRule.forUriPath();
        }

        @Override
        public String wwwAuthenticateHeader() {
            return innerRule.wwwAuthenticateHeader();
        }
    }
}
