package org.neo4j.server.rest.web;

import org.neo4j.server.database.Database;
import org.neo4j.server.extensions.BadExtensionInvocationException;
import org.neo4j.server.extensions.ExtensionInvocationFailureException;
import org.neo4j.server.extensions.ExtensionInvocator;
import org.neo4j.server.extensions.ExtensionLookupException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.OutputFormat;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;


@Path( "ext" )
public class ExtensionService
{
    private static final String PATH_EXTENSION = "/{name}";
    private static final String PATH_GRAPHDB_EXTENSION_METHOD = PATH_EXTENSION + "/graphdb/{method}";
    private static final String PATH_NODE_EXTENSION_METHOD = PATH_EXTENSION + "/node/{method}";
    private static final String PATH_RELATIONSHIP_EXTENSION_METHOD = PATH_EXTENSION + "/relationship/{method}";
    private final InputFormat input;
    private final OutputFormat output;
    private DatabaseActions server;

    public ExtensionService( @Context Database database,
                             @Context ExtensionInvocator extensions, @Context InputFormat input,
                             @Context OutputFormat output )
    {
        this.input = input;

        this.output = output;
        this.server = new DatabaseActions( database, extensions );
    }

    @GET
    public Response getExtensionsList()
    {
        return output.ok( server.getExtensionsList() );
    }

    @GET
    @Path( PATH_EXTENSION )
    public Response getExtensionList( @PathParam( "name" ) String name )
    {
        try
        {
            return output.ok( server.getExtensionList( name ) );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        }
    }

    @POST
    @Path( PATH_GRAPHDB_EXTENSION_METHOD )
    public Response invokeGraphDatabaseExtension( @PathParam( "name" ) String name,
                                                  @PathParam( "method" ) String method, String data )
    {
        try
        {
            return output.ok( server.invokeGraphDatabaseExtension( name, method,
                    input.readParameterList( data ) ) );
        } catch ( BadInputException e )
        {
            return output.badRequest( e );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        } catch ( BadExtensionInvocationException e )
        {
            return output.badRequest( e.getCause() );
        } catch ( ExtensionInvocationFailureException e )
        {
            return output.serverError( e.getCause() );
        }
    }

    @GET
    @Path( PATH_GRAPHDB_EXTENSION_METHOD )
    public Response getGraphDatabaseExtensionDescription( @PathParam( "name" ) String name,
                                                          @PathParam( "method" ) String method )
    {
        try
        {
            return output.ok( server.describeGraphDatabaseExtension( name, method ) );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        }
    }

    @POST
    @Path( PATH_NODE_EXTENSION_METHOD )
    public Response invokeNodeExtension( @PathParam( "name" ) String name,
                                         @PathParam( "method" ) String method, @PathParam( "nodeId" ) long nodeId,
                                         String data )
    {
        try
        {
            return output.ok( server.invokeNodeExtension( nodeId, name, method,
                    input.readParameterList( data ) ) );
        } catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        } catch ( BadInputException e )
        {
            return output.badRequest( e );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        } catch ( BadExtensionInvocationException e )
        {
            return output.badRequest( e.getCause() );
        } catch ( ExtensionInvocationFailureException e )
        {
            return output.serverError( e.getCause() );
        }
    }

    @GET
    @Path( PATH_NODE_EXTENSION_METHOD )
    public Response getNodeExtensionDescription( @PathParam( "name" ) String name,
                                                 @PathParam( "method" ) String method,
                                                 @PathParam( "nodeId" ) long nodeId )
    {
        try
        {
            return output.ok( server.describeNodeExtension( name, method ) );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        }
    }

    @POST
    @Path( PATH_RELATIONSHIP_EXTENSION_METHOD )
    public Response invokeRelationshipExtension( @PathParam( "name" ) String name,
                                                 @PathParam( "method" ) String method,
                                                 @PathParam( "relationshipId" ) long relationshipId, String data )
    {
        try
        {
            return output.ok( server.invokeRelationshipExtension( relationshipId, name, method,
                    input.readParameterList( data ) ) );
        } catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        } catch ( BadInputException e )
        {
            return output.badRequest( e );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        } catch ( BadExtensionInvocationException e )
        {
            return output.badRequest( e.getCause() );
        } catch ( ExtensionInvocationFailureException e )
        {
            return output.serverError( e.getCause() );
        }
    }

    @GET
    @Path( PATH_RELATIONSHIP_EXTENSION_METHOD )
    public Response getRelationshipExtensionDescription( @PathParam( "name" ) String name,
                                                         @PathParam( "method" ) String method,
                                                         @PathParam( "relationshipId" ) long relationshipId )
    {
        try
        {
            return output.ok( server.describeRelationshipExtension( name, method ) );
        } catch ( ExtensionLookupException e )
        {
            return output.notFound( e );
        }
    }
}
