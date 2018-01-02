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

import org.neo4j.server.database.InjectableProvider;

import com.sun.jersey.api.core.HttpContext;

@Provider
public final class InputFormatProvider extends InjectableProvider<InputFormat>
{
    private final RepresentationFormatRepository repository;

    public InputFormatProvider( RepresentationFormatRepository repository )
    {
        super( InputFormat.class );
        this.repository = repository;
    }

    @Override
    public InputFormat getValue( HttpContext context )
    {
        try
        {
            return repository.inputFormat( context.getRequest()
                    .getMediaType() );
        }
        catch ( MediaTypeNotSupportedException e )
        {
            throw new WebApplicationException( Response.status( Status.UNSUPPORTED_MEDIA_TYPE )
                    .entity( e.getMessage() )
                    .build() );
        }
    }
}
