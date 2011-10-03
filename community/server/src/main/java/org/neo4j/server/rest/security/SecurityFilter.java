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

    public SecurityFilter( SecurityRule rule )
    {
        this.rule = rule;
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

        if ( rule.isAuthorized( (HttpServletRequest) request ) )
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
        httpServletResponse.addHeader( "WWW-Authenticate", "Basic realm=\"" + rule.forRealm() + "\"" );
    }

    @Override
    public void destroy()
    {
    }
}
