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
package org.neo4j.server.rest.web;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.database.Database;
import org.neo4j.server.plugins.BadPluginInvocationException;
import org.neo4j.server.plugins.ParameterList;
import org.neo4j.server.plugins.PluginInvocationFailureException;
import org.neo4j.server.plugins.PluginInvocator;
import org.neo4j.server.plugins.PluginLookupException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ServerExtensionRepresentation;

@Path( "ext" )
public class ExtensionService
{
    private static final String PATH_EXTENSION = "/{name}";
    private static final String PATH_GRAPHDB_EXTENSION_METHOD = PATH_EXTENSION + "/graphdb/{method}";
    private static final String PATH_NODE_EXTENSION_METHOD = PATH_EXTENSION + "/node/{nodeId}/{method}";
    private static final String PATH_RELATIONSHIP_EXTENSION_METHOD = PATH_EXTENSION
                                                                     + "/relationship/{relationshipId}/{method}";
    private final InputFormat input;
    private final OutputFormat output;
    private final PluginInvocator extensions;
    private final GraphDatabaseAPI graphDb;

    public ExtensionService( @Context InputFormat input, @Context OutputFormat output,
            @Context PluginInvocator extensions, @Context Database database )
    {
        this.input = input;
        this.output = output;
        this.extensions = extensions;
        this.graphDb = database.getGraph();
    }

    public OutputFormat getOutputFormat()
    {
        return output;
    }

    private Node node( long id ) throws NodeNotFoundException
    {
        try(Transaction tx = graphDb.beginTx())
        {
            Node node = graphDb.getNodeById( id );

            tx.success();
            return node;
        }
        catch ( NotFoundException e )
        {
            throw new NodeNotFoundException( e );
        }
    }

    private Relationship relationship( long id ) throws RelationshipNotFoundException
    {
        try(Transaction tx = graphDb.beginTx())
        {
            Relationship relationship = graphDb.getRelationshipById( id );

            tx.success();
            return relationship;
        }
        catch ( NotFoundException e )
        {
            throw new RelationshipNotFoundException( e );
        }
    }

    @GET
    public Response getExtensionsList()
    {
        return output.ok( this.extensionsList() );
    }

    @GET
    @Path( PATH_EXTENSION )
    public Response getExtensionList( @PathParam( "name" ) String name )
    {
        try
        {
            return output.ok( this.extensionList( name ) );
        }
        catch ( PluginLookupException e )
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
            return output.ok( this.invokeGraphDatabaseExtension( name, method, input.readParameterList( data ) ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( PluginLookupException e )
        {
            return output.notFound( e );
        }
        catch ( BadPluginInvocationException e )
        {
            return output.badRequest( e.getCause() );
        }
        catch ( SyntaxException e )
        {
            return output.badRequest( e.getCause() );
        }
        catch ( PluginInvocationFailureException e )
        {
            return output.serverError( e.getCause() );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path( PATH_GRAPHDB_EXTENSION_METHOD )
    public Response getGraphDatabaseExtensionDescription( @PathParam( "name" ) String name,
            @PathParam( "method" ) String method )
    {
        try
        {
            return output.ok( this.describeGraphDatabaseExtension( name, method ) );
        }
        catch ( PluginLookupException e )
        {
            return output.notFound( e );
        }
    }

    @POST
    @Path( PATH_NODE_EXTENSION_METHOD )
    public Response invokeNodeExtension( @PathParam( "name" ) String name, @PathParam( "method" ) String method,
            @PathParam( "nodeId" ) long nodeId, String data )
    {
        try
        {
            return output.ok( this.invokeNodeExtension( nodeId, name, method, input.readParameterList( data ) ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( PluginLookupException e )
        {
            return output.notFound( e );
        }
        catch ( BadPluginInvocationException e )
        {
            return output.badRequest( e.getCause() );
        }
        catch ( PluginInvocationFailureException e )
        {
            return output.serverError( e.getCause() );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path( PATH_NODE_EXTENSION_METHOD )
    public Response getNodeExtensionDescription( @PathParam( "name" ) String name,
            @PathParam( "method" ) String method, @PathParam( "nodeId" ) long nodeId )
    {
        try
        {
            return output.ok( this.describeNodeExtension( name, method ) );
        }
        catch ( PluginLookupException e )
        {
            return output.notFound( e );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @POST
    @Path( PATH_RELATIONSHIP_EXTENSION_METHOD )
    public Response invokeRelationshipExtension( @PathParam( "name" ) String name,
            @PathParam( "method" ) String method, @PathParam( "relationshipId" ) long relationshipId, String data )
    {
        try
        {
            return output.ok( this.invokeRelationshipExtension( relationshipId, name, method,
                    input.readParameterList( data ) ) );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( PluginLookupException e )
        {
            return output.notFound( e );
        }
        catch ( BadPluginInvocationException e )
        {
            return output.badRequest( e.getCause() );
        }
        catch ( PluginInvocationFailureException e )
        {
            return output.serverError( e.getCause() );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path( PATH_RELATIONSHIP_EXTENSION_METHOD )
    public Response getRelationshipExtensionDescription( @PathParam( "name" ) String name,
            @PathParam( "method" ) String method, @PathParam( "relationshipId" ) long relationshipId )
    {
        try
        {
            return output.ok( this.describeRelationshipExtension( name, method ) );
        }
        catch ( PluginLookupException e )
        {
            return output.notFound( e );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    // Extensions

    protected Representation extensionsList()
    {
        return new MappingRepresentation( "extensions" )
        {
            @Override
            protected void serialize( MappingSerializer serializer )
            {
                for ( String extension : extensions.extensionNames() )
                {
                    serializer.putUri( extension, "ext/" + extension );
                }
            }
        };
    }

    protected Representation extensionList( String extensionName ) throws PluginLookupException
    {
        return new ServerExtensionRepresentation( extensionName, extensions.describeAll( extensionName ) );
    }

    protected Representation invokeGraphDatabaseExtension( String extensionName, String method, ParameterList data )
            throws PluginLookupException, BadInputException, PluginInvocationFailureException,
            BadPluginInvocationException
    {
        return extensions.invoke( graphDb, extensionName, GraphDatabaseService.class, method, graphDb, data );
    }

    protected Representation describeGraphDatabaseExtension( String extensionName, String method )
            throws PluginLookupException
    {
        return extensions.describe( extensionName, GraphDatabaseService.class, method );
    }

    protected Representation invokeNodeExtension( long nodeId, String extensionName, String method, ParameterList data )
            throws NodeNotFoundException, PluginLookupException, BadInputException, PluginInvocationFailureException,
            BadPluginInvocationException
    {
        return extensions.invoke( graphDb, extensionName, Node.class, method, node( nodeId ), data );
    }

    protected Representation describeNodeExtension( String extensionName, String method ) throws PluginLookupException
    {
        return extensions.describe( extensionName, Node.class, method );
    }

    protected Representation invokeRelationshipExtension( long relationshipId, String extensionName, String method,
            ParameterList data ) throws RelationshipNotFoundException, PluginLookupException, BadInputException,
            PluginInvocationFailureException, BadPluginInvocationException
    {
        return extensions.invoke( graphDb, extensionName, Relationship.class, method, relationship( relationshipId ),
                data );
    }

    protected Representation describeRelationshipExtension( String extensionName, String method )
            throws PluginLookupException
    {
        return extensions.describe( extensionName, Relationship.class, method );
    }
}
