package org.neo4j.server.web;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class NoCacheHtmlFilter implements Filter
{
    @Override
    public void init( FilterConfig filterConfig ) throws ServletException
    {
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain )
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        if ( request.getServletPath() != null && request.getServletPath().endsWith( ".html" ))
        {
            response.addHeader( "Cache-Control", "no-cache" );
        }
        filterChain.doFilter( servletRequest, servletResponse);
    }

    @Override
    public void destroy()
    {
    }
}
