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
package org.neo4j.server.rest.repr;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;

import org.neo4j.server.database.InjectableProvider;

@Provider
public final class OutputFormatProvider extends InjectableProvider<OutputFormat>
{
    private final RepresentationFormatRepository repository;

    public OutputFormatProvider( RepresentationFormatRepository repository )
    {
        super( OutputFormat.class );
        this.repository = repository;
    }

    @Override
    public OutputFormat getValue( HttpContext context )
    {
        try
        {
            HttpRequestContext request = context.getRequest();
            return repository.outputFormat( request.getAcceptableMediaTypes(),
                    request.getBaseUri(),
                    request.getRequestHeaders() );
        }
        catch ( MediaTypeNotSupportedException e )
        {
            throw new WebApplicationException( Response.status( Status.NOT_ACCEPTABLE )
                    .entity( e.getMessage() )
                    .build() );
        }
    }
}
