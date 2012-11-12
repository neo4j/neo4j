/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.webadmin.rest;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.neo4j.helpers.Pair;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.server.webadmin.rest.representations.ServiceDefinitionRepresentation;

@Path( ConsoleService.SERVICE_PATH )
public class ConsoleService implements AdvertisableService
{
    private static final String SERVICE_NAME = "console";
    static final String SERVICE_PATH = "server/console";
    private final SessionFactory sessionFactory;
    private final Database database;
    private final OutputFormat output;

    public ConsoleService( SessionFactory sessionFactory, Database database, OutputFormat output )
    {
        this.sessionFactory = sessionFactory;
        this.database = database;
        this.output = output;
    }

    public ConsoleService( @Context Database database, @Context HttpServletRequest req, @Context OutputFormat output )
    {
        this( new SessionFactoryImpl( req.getSession( true ) ), database, output );
    }

    Logger log = Logger.getLogger( ConsoleService.class );

    @Override
    public String getName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServerPath()
    {
        return SERVICE_PATH;
    }

    @GET
    public Response getServiceDefinition()
    {
        ServiceDefinitionRepresentation result = new ServiceDefinitionRepresentation( SERVICE_PATH );
        result.resourceUri( "exec", "" );

        return output.ok( result );
    }

    @POST
    public Response exec( @Context InputFormat input, String data )
    {
        Map<String, Object> args;
        try
        {
            args = input.readMap( data );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }

        if ( !args.containsKey( "command" ) )
        {
            return Response.status( Status.BAD_REQUEST )
                    .entity( "Expected command argument not present." )
                    .build();
        }

        ScriptSession scriptSession = getSession( args );
        log.trace( scriptSession.toString() );
        try
        {
            Pair<String, String> result = scriptSession.evaluate( (String) args.get( "command" ) );
            List<Representation> list = new ArrayList<Representation>(
                    asList( ValueRepresentation.string( result.first() ), ValueRepresentation.string( result.other() ) ) );
            
            return output.ok( new ListRepresentation( RepresentationType.STRING, list ) );
        } catch (Exception e)
        {
            List<Representation> list = new ArrayList<Representation>(
                    asList( ValueRepresentation.string( e.getClass() + " : " + e.getMessage() + "\n"), ValueRepresentation.string( null ) ));
            return output.ok(new ListRepresentation( RepresentationType.STRING, list ));
        }
    }

    private ScriptSession getSession( Map<String, Object> args )
    {
        return sessionFactory.createSession( (String) args.get( "engine" ), database );
    }
}
