/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.ws.rs.core.Response;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toUnmodifiableList;

public class AccessiblePathFilter extends AbstractFilter
{
    private final List<Pattern> blacklist;
    private final Log log;

    public AccessiblePathFilter( LogProvider logProvider, List<String> blacklist )
    {
        this.blacklist = blacklist.stream().map( Pattern::compile ).collect( toUnmodifiableList() );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void doFilter( ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain ) throws IOException, ServletException
    {
        var request = validateRequestType( servletRequest );
        var response = validateResponseType( servletResponse );
        var path = request.getContextPath() + ( request.getPathInfo() == null ? "" : request.getPathInfo() );
        if ( blacklisted( path ) )
        {
            log.debug( "HTTP client '%s' trying to access a disabled server path: '%s'.", request.getRemoteAddr(), path );
            response.setStatus( Response.Status.FORBIDDEN.getStatusCode() );
        }
        else
        {
            chain.doFilter( servletRequest, servletResponse );
        }
    }

    private boolean blacklisted( String path )
    {
        for ( Pattern pattern : blacklist )
        {
            if ( pattern.matcher( path ).matches() )
            {
                return true;
            }
        }
        return false;
    }
}
