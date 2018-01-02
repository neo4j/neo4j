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

import org.apache.commons.configuration.Configuration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.Pair;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.EvaluationException;
import org.neo4j.server.rest.domain.PropertySettingStrategy;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.IndexedEntityRepresentation;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.server.rest.repr.ListEntityRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.toMap;
import static org.neo4j.server.rest.web.Surface.PATH_LABELS;
import static org.neo4j.server.rest.web.Surface.PATH_NODES;
import static org.neo4j.server.rest.web.Surface.PATH_NODE_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIPS;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIP_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_CONSTRAINT;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_RELATIONSHIP_CONSTRAINT;

@Path( "/" )
public class RestfulGraphDatabase
{
    @SuppressWarnings("serial")
    public static class AmpersandSeparatedCollection extends LinkedHashSet<String>
    {
        public AmpersandSeparatedCollection( String path )
        {
            for ( String e : path.split( "&" ) )
            {
                if ( e.trim()
                        .length() > 0 )
                {
                    add( e );
                }
            }
        }
    }

    private static final String PATH_NODE = PATH_NODES + "/{nodeId}";
    private static final String PATH_NODE_PROPERTIES = PATH_NODE + "/properties";
    private static final String PATH_NODE_PROPERTY = PATH_NODE_PROPERTIES + "/{key}";
    private static final String PATH_NODE_RELATIONSHIPS = PATH_NODE + "/relationships";
    private static final String PATH_RELATIONSHIP = PATH_RELATIONSHIPS + "/{relationshipId}";
    private static final String PATH_NODE_RELATIONSHIPS_W_DIR = PATH_NODE_RELATIONSHIPS + "/{direction}";
    private static final String PATH_NODE_RELATIONSHIPS_W_DIR_N_TYPES = PATH_NODE_RELATIONSHIPS_W_DIR + "/{types}";
    private static final String PATH_RELATIONSHIP_PROPERTIES = PATH_RELATIONSHIP + "/properties";
    private static final String PATH_RELATIONSHIP_PROPERTY = PATH_RELATIONSHIP_PROPERTIES + "/{key}";
    private static final String PATH_NODE_TRAVERSE = PATH_NODE + "/traverse/{returnType}";
    private static final String PATH_NODE_PATH = PATH_NODE + "/path";
    private static final String PATH_NODE_PATHS = PATH_NODE + "/paths";
    private static final String PATH_NODE_LABELS = PATH_NODE + "/labels";
    private static final String PATH_NODE_LABEL = PATH_NODE + "/labels/{label}";
    private static final String PATH_NODE_DEGREE = PATH_NODE + "/degree";
    private static final String PATH_NODE_DEGREE_W_DIR = PATH_NODE_DEGREE + "/{direction}";
    private static final String PATH_NODE_DEGREE_W_DIR_N_TYPES = PATH_NODE_DEGREE_W_DIR + "/{types}";

    private static final String PATH_PROPERTY_KEYS = "propertykeys";

    protected static final String PATH_NAMED_NODE_INDEX = PATH_NODE_INDEX + "/{indexName}";
    protected static final String PATH_NODE_INDEX_GET = PATH_NAMED_NODE_INDEX + "/{key}/{value}";
    protected static final String PATH_NODE_INDEX_QUERY_WITH_KEY = PATH_NAMED_NODE_INDEX + "/{key}"; //
    // http://localhost/db/data/index/node/foo?query=somelucenestuff
    protected static final String PATH_NODE_INDEX_ID = PATH_NODE_INDEX_GET + "/{id}";
    protected static final String PATH_NODE_INDEX_REMOVE_KEY = PATH_NAMED_NODE_INDEX + "/{key}/{id}";
    protected static final String PATH_NODE_INDEX_REMOVE = PATH_NAMED_NODE_INDEX + "/{id}";

    protected static final String PATH_NAMED_RELATIONSHIP_INDEX = PATH_RELATIONSHIP_INDEX + "/{indexName}";
    protected static final String PATH_RELATIONSHIP_INDEX_GET = PATH_NAMED_RELATIONSHIP_INDEX + "/{key}/{value}";
    protected static final String PATH_RELATIONSHIP_INDEX_QUERY_WITH_KEY = PATH_NAMED_RELATIONSHIP_INDEX + "/{key}";
    protected static final String PATH_RELATIONSHIP_INDEX_ID = PATH_RELATIONSHIP_INDEX_GET + "/{id}";
    protected static final String PATH_RELATIONSHIP_INDEX_REMOVE_KEY = PATH_NAMED_RELATIONSHIP_INDEX + "/{key}/{id}";
    protected static final String PATH_RELATIONSHIP_INDEX_REMOVE = PATH_NAMED_RELATIONSHIP_INDEX + "/{id}";

    public static final String PATH_AUTO_INDEX = "index/auto/{type}";
    protected static final String PATH_AUTO_INDEX_STATUS = PATH_AUTO_INDEX + "/status";
    protected static final String PATH_AUTO_INDEXED_PROPERTIES = PATH_AUTO_INDEX + "/properties";
    protected static final String PATH_AUTO_INDEX_PROPERTY_DELETE = PATH_AUTO_INDEXED_PROPERTIES + "/{property}";
    protected static final String PATH_AUTO_INDEX_GET = PATH_AUTO_INDEX + "/{key}/{value}";

    public static final String PATH_ALL_NODES_LABELED = "label/{label}/nodes";

    public static final String PATH_SCHEMA_INDEX_LABEL = PATH_SCHEMA_INDEX + "/{label}";
    public static final String PATH_SCHEMA_INDEX_LABEL_PROPERTY = PATH_SCHEMA_INDEX_LABEL + "/{property}";

    public static final String PATH_SCHEMA_CONSTRAINT_LABEL = PATH_SCHEMA_CONSTRAINT + "/{label}";
    public static final String PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS = PATH_SCHEMA_CONSTRAINT_LABEL + "/uniqueness";
    public static final String PATH_SCHEMA_CONSTRAINT_LABEL_EXISTENCE = PATH_SCHEMA_CONSTRAINT_LABEL + "/existence";
    public static final String PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS_PROPERTY = PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS + "/{property}";
    public static final String PATH_SCHEMA_CONSTRAINT_LABEL_EXISTENCE_PROPERTY = PATH_SCHEMA_CONSTRAINT_LABEL_EXISTENCE + "/{property}";

    public static final String PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_TYPE = PATH_SCHEMA_RELATIONSHIP_CONSTRAINT + "/{type}";
    public static final String PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_TYPE_EXISTENCE = PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_TYPE + "/existence";
    public static final String PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_EXISTENCE_PROPERTY = PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_TYPE_EXISTENCE + "/{property}";

    public static final String NODE_AUTO_INDEX_TYPE = "node";
    public static final String RELATIONSHIP_AUTO_INDEX_TYPE = "relationship";

    private static final String SIXTY_SECONDS = "60";
    private static final String FIFTY_ENTRIES = "50";

    private static final String UNIQUENESS_MODE_GET_OR_CREATE = "get_or_create";
    private static final String UNIQUENESS_MODE_CREATE_OR_FAIL = "create_or_fail";

    // TODO Obviously change name/content on this
    private static final String HEADER_TRANSACTION = "Transaction";

    private final DatabaseActions actions;
    private Configuration config;
    private final OutputFormat output;
    private final InputFormat input;

    public static final String PATH_TO_CREATE_PAGED_TRAVERSERS = PATH_NODE + "/paged/traverse/{returnType}";
    public static final String PATH_TO_PAGED_TRAVERSERS = PATH_NODE + "/paged/traverse/{returnType}/{traverserId}";

    private enum UniqueIndexType
    {
        None,
        GetOrCreate,
        CreateOrFail
    }

    public RestfulGraphDatabase( @Context InputFormat input,
                                 @Context OutputFormat output,
                                 @Context DatabaseActions actions,
                                 @Context Configuration config )
    {
        this.input = input;
        this.output = output;
        this.actions = actions;
        this.config = config;
    }

    public OutputFormat getOutputFormat()
    {
        return output;
    }

    private Response nothing()
    {
        return output.noContent();
    }

    private Long extractNodeIdOrNull( String uri ) throws BadInputException
    {
        if ( uri == null )
        {
            return null;
        }
        return extractNodeId( uri );
    }

    private long extractNodeId( String uri ) throws BadInputException
    {
        try
        {
            return Long.parseLong( uri.substring( uri.lastIndexOf( "/" ) + 1 ) );
        }
        catch ( NumberFormatException | NullPointerException ex )
        {
            throw new BadInputException( ex );
        }
    }

    private Long extractRelationshipIdOrNull( String uri ) throws BadInputException
    {
        if ( uri == null )
        {
            return null;
        }
        return extractRelationshipId( uri );
    }

    private long extractRelationshipId( String uri ) throws BadInputException
    {
        return extractNodeId( uri );
    }

    @GET
    public Response getRoot()
    {
        return output.ok( actions.root() );
    }

    // Nodes

    @POST
    @Path(PATH_NODES)
    public Response createNode( String body )
    {
        try
        {
            return output.created( actions.createNode( input.readMap( body ) ) );
        }
        catch ( ArrayStoreException ase )
        {
            return generateBadRequestDueToMangledJsonResponse( body );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ClassCastException e )
        {
            return output.badRequest( e );
        }
    }

    private Response generateBadRequestDueToMangledJsonResponse( String body )
    {
        return output.badRequest( MediaType.TEXT_PLAIN_TYPE, "Invalid JSON array in POST body: " + body );
    }

    @GET
    @Path(PATH_NODE)
    public Response getNode( @PathParam("nodeId") long nodeId )
    {
        try
        {
            return output.ok( actions.getNode( nodeId ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @DELETE
    @Path(PATH_NODE)
    public Response deleteNode( @PathParam("nodeId") long nodeId )
    {
        try
        {
            actions.deleteNode( nodeId );
            return nothing();
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    // Node properties

    @PUT
    @Path(PATH_NODE_PROPERTIES)
    public Response setAllNodeProperties( @PathParam("nodeId") long nodeId, String body )
    {
        try
        {
            actions.setAllNodeProperties( nodeId, input.readMap( body ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ArrayStoreException ase )
        {
            return generateBadRequestDueToMangledJsonResponse( body );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
        return nothing();
    }

    @GET
    @Path(PATH_NODE_PROPERTIES)
    public Response getAllNodeProperties( @PathParam("nodeId") long nodeId )
    {
        try
        {
            return output.response( Response.Status.OK, actions.getAllNodeProperties( nodeId ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @PUT
    @Path(PATH_NODE_PROPERTY)
    public Response setNodeProperty( @PathParam("nodeId") long nodeId,
                                     @PathParam("key") String key, String body )
    {
        try
        {
            actions.setNodeProperty( nodeId, key, input.readValue( body ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ArrayStoreException ase )
        {
            return generateBadRequestDueToMangledJsonResponse( body );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e)
        {
            return output.conflict( e );
        }
        return nothing();
    }

    @GET
    @Path(PATH_NODE_PROPERTY)
    public Response getNodeProperty( @PathParam("nodeId") long nodeId, @PathParam("key") String key )
    {
        try
        {
            return output.ok( actions.getNodeProperty( nodeId, key ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
    }

    @DELETE
    @Path(PATH_NODE_PROPERTY)
    public Response deleteNodeProperty( @PathParam("nodeId") long nodeId, @PathParam("key") String key )
    {
        try
        {
            actions.removeNodeProperty( nodeId, key );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @DELETE
    @Path(PATH_NODE_PROPERTIES)
    public Response deleteAllNodeProperties( @PathParam("nodeId") long nodeId )
    {
        try
        {
            actions.removeAllNodeProperties( nodeId );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( PropertyValueException e )
        {
            return output.badRequest( e );
        }
        return nothing();
    }

    // Node Labels

    @POST
    @Path( PATH_NODE_LABELS )
    public Response addNodeLabel( @PathParam( "nodeId" ) long nodeId, String body )
    {
        try
        {
            Object rawInput = input.readValue( body );
            if ( rawInput instanceof String )
            {
                ArrayList<String> s = new ArrayList<>();
                s.add((String) rawInput);
                actions.addLabelToNode( nodeId, s );
            }
            else if(rawInput instanceof Collection)
            {
                actions.addLabelToNode( nodeId, (Collection<String>) rawInput );
            }
            else
            {
                throw new InvalidArgumentsException( format( "Label name must be a string. Got: '%s'", rawInput ) );
            }
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ArrayStoreException ase )
        {
            return generateBadRequestDueToMangledJsonResponse( body );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @PUT
    @Path( PATH_NODE_LABELS )
    public Response setNodeLabels( @PathParam( "nodeId" ) long nodeId, String body )
    {
        try
        {
            Object rawInput = input.readValue( body );
            if ( !(rawInput instanceof Collection) )
            {
                throw new InvalidArgumentsException( format( "Input must be an array of Strings. Got: '%s'", rawInput ) );
            }
            else
            {
                actions.setLabelsOnNode( nodeId, (Collection<String>) rawInput );
            }
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ArrayStoreException ase )
        {
            return generateBadRequestDueToMangledJsonResponse( body );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @DELETE
    @Path( PATH_NODE_LABEL )
    public Response removeNodeLabel( @PathParam( "nodeId" ) long nodeId, @PathParam( "label" ) String labelName )
    {
        try
        {
            actions.removeLabelFromNode( nodeId, labelName );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @GET
    @Path( PATH_NODE_LABELS )
    public Response getNodeLabels( @PathParam( "nodeId" ) long nodeId )
    {
        try
        {
            return output.ok( actions.getNodeLabels( nodeId ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path( PATH_ALL_NODES_LABELED )
    public Response getNodesWithLabelAndProperty( @PathParam("label") String labelName, @Context UriInfo uriInfo )
    {
        try
        {
            if ( labelName.isEmpty() )
            {
                throw new InvalidArgumentsException( "Empty label name" );
            }

            Map<String, Object> properties = toMap( map( queryParamsToProperties, uriInfo.getQueryParameters().entrySet()));

            return output.ok( actions.getNodesWithLabel( labelName, properties ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
    }

    @GET
    @Path( PATH_LABELS )
    public Response getAllLabels( @QueryParam( "in_use" ) @DefaultValue( "true" ) boolean inUse )
    {
        return output.ok( actions.getAllLabels( inUse ) );
    }

    // Property keys

    @GET
    @Path( PATH_PROPERTY_KEYS )
    public Response getAllPropertyKeys( )
    {
        return output.ok( actions.getAllPropertyKeys() );
    }

    // Relationships

    @SuppressWarnings("unchecked")
    @POST
    @Path(PATH_NODE_RELATIONSHIPS)
    public Response createRelationship( @PathParam("nodeId") long startNodeId, String body )
    {
        final Map<String, Object> data;
        final long endNodeId;
        final String type;
        final Map<String, Object> properties;
        try
        {
            data = input.readMap( body );
            endNodeId = extractNodeId( (String) data.get( "to" ) );
            type = (String) data.get( "type" );
            properties = (Map<String, Object>) data.get( "data" );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ClassCastException e )
        {
            return output.badRequest( e );
        }
        try
        {
            return output.created( actions.createRelationship( startNodeId, endNodeId, type, properties ) );
        }
        catch ( StartNodeNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( EndNodeNotFoundException e )
        {
            return output.badRequest( e );
        }
        catch ( PropertyValueException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP)
    public Response getRelationship( @PathParam("relationshipId") long relationshipId )
    {
        try
        {
            return output.ok( actions.getRelationship( relationshipId ) );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @DELETE
    @Path(PATH_RELATIONSHIP)
    public Response deleteRelationship( @PathParam("relationshipId") long relationshipId )
    {
        try
        {
            actions.deleteRelationship( relationshipId );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @GET
    @Path(PATH_NODE_RELATIONSHIPS_W_DIR)
    public Response getNodeRelationships( @PathParam("nodeId") long nodeId,
                                          @PathParam("direction") RelationshipDirection direction )
    {
        try
        {
            return output.ok( actions.getNodeRelationships( nodeId, direction, Collections.<String>emptyList() ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path(PATH_NODE_RELATIONSHIPS_W_DIR_N_TYPES)
    public Response getNodeRelationships( @PathParam("nodeId") long nodeId,
                                          @PathParam("direction") RelationshipDirection direction,
                                          @PathParam("types") AmpersandSeparatedCollection types )
    {
        try
        {
            return output.ok( actions.getNodeRelationships( nodeId, direction, types ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    // Degrees

    @GET
    @Path(PATH_NODE_DEGREE_W_DIR)
    public Response getNodeDegree( @PathParam("nodeId") long nodeId,
                                   @PathParam("direction") RelationshipDirection direction )
    {
        try
        {
            return output.ok( actions.getNodeDegree( nodeId, direction, Collections.<String>emptyList() ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path(PATH_NODE_DEGREE_W_DIR_N_TYPES)
    public Response getNodeDegree(@PathParam("nodeId") long nodeId,
                                  @PathParam("direction") RelationshipDirection direction,
                                  @PathParam("types") AmpersandSeparatedCollection types )
    {
        try
        {
            return output.ok( actions.getNodeDegree( nodeId, direction, types ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    // Relationship properties

    @GET
    @Path(PATH_RELATIONSHIP_PROPERTIES)
    public Response getAllRelationshipProperties( @PathParam("relationshipId") long relationshipId )
    {
        try
        {
            return output.response( Response.Status.OK, actions.getAllRelationshipProperties( relationshipId ) );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_PROPERTY)
    public Response getRelationshipProperty( @PathParam("relationshipId") long relationshipId,
                                             @PathParam("key") String key )
    {
        try
        {
            return output.ok( actions.getRelationshipProperty( relationshipId, key ) );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
    }

    @PUT
    @Path(PATH_RELATIONSHIP_PROPERTIES)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setAllRelationshipProperties( @PathParam("relationshipId") long relationshipId, String body )
    {
        try
        {
            actions.setAllRelationshipProperties( relationshipId, input.readMap( body ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @PUT
    @Path(PATH_RELATIONSHIP_PROPERTY)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setRelationshipProperty( @PathParam("relationshipId") long relationshipId,
                                             @PathParam("key") String key, String body )
    {
        try
        {
            actions.setRelationshipProperty( relationshipId, key, input.readValue( body ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_PROPERTIES)
    public Response deleteAllRelationshipProperties( @PathParam("relationshipId") long relationshipId )
    {
        try
        {
            actions.removeAllRelationshipProperties( relationshipId );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( PropertyValueException e )
        {
            return output.badRequest( e );
        }
        return nothing();
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_PROPERTY)
    public Response deleteRelationshipProperty( @PathParam("relationshipId") long relationshipId,
                                                @PathParam("key") String key )
    {
        try
        {
            actions.removeRelationshipProperty( relationshipId, key );
        }
        catch ( RelationshipNotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    // Index

    @GET
    @Path(PATH_NODE_INDEX)
    public Response getNodeIndexRoot()
    {
        if ( actions.getNodeIndexNames().length == 0 )
        {
            return output.noContent();
        }
        return output.ok( actions.nodeIndexRoot() );
    }

    @POST
    @Path(PATH_NODE_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response jsonCreateNodeIndex(  String json )
    {
        try
        {
            return output.created( actions.createNodeIndex( input.readMap( json ) ) );
        }
        catch ( IllegalArgumentException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX)
    public Response getRelationshipIndexRoot()
    {
        if ( actions.getRelationshipIndexNames().length == 0 )
        {
            return output.noContent();
        }
        return output.ok( actions.relationshipIndexRoot() );
    }

    @POST
    @Path(PATH_RELATIONSHIP_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response jsonCreateRelationshipIndex(  String json )
    {
        try
        {
            return output.created( actions.createRelationshipIndex( input.readMap( json ) ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( IllegalArgumentException e )
        {
            return output.badRequest( e );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_NAMED_NODE_INDEX)
    public Response getIndexedNodesByQuery( @PathParam("indexName") String indexName,
                                            @QueryParam("query") String query,
                                            @QueryParam("order") String order )
    {
        try
        {
            return output.ok( actions.getIndexedNodesByQuery( indexName, query,
                    order ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_AUTO_INDEX)
    public Response getAutoIndexedNodesByQuery( @PathParam("type") String type, @QueryParam("query") String query )
    {
        try
        {
            if ( type.equals( NODE_AUTO_INDEX_TYPE ) )
            {
                return output.ok( actions.getAutoIndexedNodesByQuery( query ) );
            }
            else if ( type.equals( RELATIONSHIP_AUTO_INDEX_TYPE ) )
            {
                return output.ok( actions.getAutoIndexedRelationshipsByQuery( query ) );
            }
            else
            {
                return output.badRequest( new RuntimeException( "Unrecognized auto-index type, " +
                        "expected '" + NODE_AUTO_INDEX_TYPE + "' or '" + RELATIONSHIP_AUTO_INDEX_TYPE + "'" ) );
            }
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_NAMED_NODE_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteNodeIndex(
                                     @PathParam("indexName") String indexName )
    {
        try
        {
            actions.removeNodeIndex( indexName );
            return output.noContent();
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
    }

    @DELETE
    @Path(PATH_NAMED_RELATIONSHIP_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteRelationshipIndex( @PathParam("indexName") String indexName )
    {
        try
        {
            actions.removeRelationshipIndex( indexName );
            return output.noContent();
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
    }

    @POST
    @Path(PATH_NAMED_NODE_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addToNodeIndex( @PathParam("indexName") String indexName, @QueryParam("unique") String unique,
                                    @QueryParam("uniqueness") String uniqueness, String postBody )
    {
        int otherHeaders = 512;
        int maximumSizeInBytes = config.getInt( ServerSettings.maximum_response_header_size.name() ) - otherHeaders;

        try
        {
            Map<String, Object> entityBody;
            Pair<IndexedEntityRepresentation, Boolean> result;

            switch ( unique( unique, uniqueness ) )
            {
                case GetOrCreate:
                    entityBody = input.readMap( postBody, "key", "value" );

                    String getOrCreateValue = String.valueOf( entityBody.get( "value" ) );
                    if ( getOrCreateValue.length() > maximumSizeInBytes )
                    {
                        return valueTooBig();
                    }

                    result = actions.getOrCreateIndexedNode( indexName,
                            String.valueOf( entityBody.get( "key" ) ),
                            getOrCreateValue, extractNodeIdOrNull( getStringOrNull(
                            entityBody, "uri" ) ), getMapOrNull( entityBody, "properties" ) );
                    return result.other() ? output.created( result.first() ) : output.okIncludeLocation( result.first
                            () );

                case CreateOrFail:
                    entityBody = input.readMap( postBody, "key", "value" );

                    String createOrFailValue = String.valueOf( entityBody.get( "value" ) );
                    if ( createOrFailValue.length() > maximumSizeInBytes )
                    {
                        return valueTooBig();
                    }

                    result = actions.getOrCreateIndexedNode( indexName,
                            String.valueOf( entityBody.get( "key" ) ),
                            createOrFailValue, extractNodeIdOrNull( getStringOrNull(
                            entityBody, "uri" ) ), getMapOrNull( entityBody, "properties" ) );
                    if ( result.other() )
                    {
                        return output.created( result.first() );
                    }

                    String uri = getStringOrNull( entityBody, "uri" );

                    if ( uri == null )
                    {
                        return output.conflict( result.first() );
                    }

                    long idOfNodeToBeIndexed = extractNodeId( uri );
                    long idOfNodeAlreadyInIndex = extractNodeId( result.first().getIdentity() );

                    if ( idOfNodeToBeIndexed == idOfNodeAlreadyInIndex )
                    {
                        return output.created( result.first() );
                    }

                    return output.conflict( result.first() );

                default:
                    entityBody = input.readMap( postBody, "key", "value", "uri" );
                    String value = String.valueOf( entityBody.get( "value" ) );

                    if ( value.length() > maximumSizeInBytes )
                    {
                        return valueTooBig();
                    }

                    return output.created( actions.addToNodeIndex( indexName,
                            String.valueOf( entityBody.get( "key" ) ),
                            value, extractNodeId( entityBody.get( "uri" )
                            .toString() ) ) );

            }
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( IllegalArgumentException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    private Response valueTooBig()
    {
        return Response.status( 413 ).entity( String.format(
                "The property value provided was too large. The maximum size is currently set to %d bytes. " +
                "You can configure this by setting the '%s' property.",
                config.getInt(ServerSettings.maximum_response_header_size.name()),
                ServerSettings.maximum_response_header_size.name() ) ).build();
    }

    @POST
    @Path(PATH_NAMED_RELATIONSHIP_INDEX)
    public Response addToRelationshipIndex( @PathParam("indexName") String indexName,
                                            @QueryParam("unique") String unique, @QueryParam("uniqueness") String
            uniqueness, String postBody )
    {
        try
        {
            Map<String, Object> entityBody;
            Pair<IndexedEntityRepresentation, Boolean> result;

            switch ( unique( unique, uniqueness ) )
            {
                case GetOrCreate:
                    entityBody = input.readMap( postBody, "key", "value" );
                    result = actions.getOrCreateIndexedRelationship( indexName,
                            String.valueOf( entityBody.get( "key" ) ),
                            String.valueOf( entityBody.get( "value" ) ), extractRelationshipIdOrNull( getStringOrNull
                            ( entityBody, "uri" ) ),
                            extractNodeIdOrNull( getStringOrNull( entityBody, "start" ) ),
                            getStringOrNull( entityBody, "type" ), extractNodeIdOrNull( getStringOrNull( entityBody,
                            "end" ) ),
                            getMapOrNull( entityBody, "properties" ) );
                    return result.other() ? output.created( result.first() ) : output.ok( result.first() );

                case CreateOrFail:
                    entityBody = input.readMap( postBody, "key", "value" );
                    result = actions.getOrCreateIndexedRelationship( indexName,
                            String.valueOf( entityBody.get( "key" ) ),
                            String.valueOf( entityBody.get( "value" ) ), extractRelationshipIdOrNull( getStringOrNull
                            ( entityBody, "uri" ) ),
                            extractNodeIdOrNull( getStringOrNull( entityBody, "start" ) ),
                            getStringOrNull( entityBody, "type" ), extractNodeIdOrNull( getStringOrNull( entityBody,
                            "end" ) ),
                            getMapOrNull( entityBody, "properties" ) );
                    if ( result.other() )
                    {
                        return output.created( result.first() );
                    }

                    String uri = getStringOrNull( entityBody, "uri" );

                    if ( uri == null )
                    {
                        return output.conflict( result.first() );
                    }

                    long idOfRelationshipToBeIndexed = extractRelationshipId( uri );
                    long idOfRelationshipAlreadyInIndex = extractRelationshipId( result.first().getIdentity() );

                    if ( idOfRelationshipToBeIndexed == idOfRelationshipAlreadyInIndex )
                    {
                        return output.created( result.first() );
                    }

                    return output.conflict( result.first() );

                default:
                    entityBody = input.readMap( postBody, "key", "value", "uri" );
                    return output.created( actions.addToRelationshipIndex( indexName,
                            String.valueOf( entityBody.get( "key" ) ),
                            String.valueOf( entityBody.get( "value" ) ), extractRelationshipId( entityBody.get( "uri"
                    ).toString() ) ) );

            }
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( IllegalArgumentException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    private UniqueIndexType unique( String uniqueParam, String uniquenessParam )
    {
        UniqueIndexType unique = UniqueIndexType.None;
        if ( uniquenessParam == null || uniquenessParam.equals( "" ) )
        {
            // Backward compatibility check
            if ( "".equals( uniqueParam ) || Boolean.parseBoolean( uniqueParam ) )
            {
                unique = UniqueIndexType.GetOrCreate;
            }

        }
        else if ( UNIQUENESS_MODE_GET_OR_CREATE.equalsIgnoreCase( uniquenessParam ) )
        {
            unique = UniqueIndexType.GetOrCreate;

        }
        else if ( UNIQUENESS_MODE_CREATE_OR_FAIL.equalsIgnoreCase( uniquenessParam ) )
        {
            unique = UniqueIndexType.CreateOrFail;

        }

        return unique;
    }

    private String getStringOrNull( Map<String, Object> map, String key ) throws BadInputException
    {
        Object object = map.get( key );
        if ( object instanceof String )
        {
            return (String) object;
        }
        if ( object == null )
        {
            return null;
        }
        throw new InvalidArgumentsException( "\"" + key + "\" should be a string" );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMapOrNull( Map<String, Object> data, String key ) throws BadInputException
    {
        Object object = data.get( key );
        if ( object instanceof Map<?, ?> )
        {
            return (Map<String, Object>) object;
        }
        if ( object == null )
        {
            return null;
        }
        throw new InvalidArgumentsException( "\"" + key + "\" should be a map" );
    }

    @GET
    @Path(PATH_NODE_INDEX_ID)
    public Response getNodeFromIndexUri( @PathParam("indexName") String indexName,
                                         @PathParam("key") String key, @PathParam("value") String value,
                                         @PathParam("id") long id )
    {
        try
        {
            return output.ok( actions.getIndexedNode( indexName, key, value, id ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX_ID)
    public Response getRelationshipFromIndexUri( @PathParam("indexName") String indexName,
                                                 @PathParam("key") String key, @PathParam("value") String value,
                                                 @PathParam("id") long id )
    {
        return output.ok( actions.getIndexedRelationship( indexName, key, value, id ) );
    }

    @GET
    @Path(PATH_NODE_INDEX_GET)
    public Response getIndexedNodes( @PathParam("indexName") String indexName,
                                     @PathParam("key") String key, @PathParam("value") String value )
    {
        try
        {
            return output.ok( actions.getIndexedNodes( indexName, key, value ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_AUTO_INDEX_GET)
    public Response getAutoIndexedNodes( @PathParam("type") String type, @PathParam("key") String key,
                                         @PathParam("value") String value )
    {
        try
        {
            if ( type.equals( NODE_AUTO_INDEX_TYPE ) )
            {
                return output.ok( actions.getAutoIndexedNodes( key, value ) );
            }
            else if ( type.equals( RELATIONSHIP_AUTO_INDEX_TYPE ) )
            {
                return output.ok( actions.getAutoIndexedRelationships( key, value ) );
            }
            else
            {
                return output.badRequest( new RuntimeException( "Unrecognized auto-index type, " +
                        "expected '" + NODE_AUTO_INDEX_TYPE + "' or '" + RELATIONSHIP_AUTO_INDEX_TYPE + "'" ) );
            }
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_NODE_INDEX_QUERY_WITH_KEY)
    public Response getIndexedNodesByQuery(
            @PathParam("indexName") String indexName,
            @PathParam("key") String key,
            @QueryParam("query") String query,
            @PathParam("order") String order )
    {
        try
        {
            return output.ok( actions.getIndexedNodesByQuery( indexName, key,
                    query, order ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX_GET)
    public Response getIndexedRelationships( @PathParam("indexName") String indexName,
                                             @PathParam("key") String key, @PathParam("value") String value )
    {
        try
        {
            return output.ok( actions.getIndexedRelationships( indexName, key, value ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_AUTO_INDEX_STATUS)
    public Response isAutoIndexerEnabled( @PathParam("type") String type )
    {
        return output.ok( actions.isAutoIndexerEnabled( type ) );
    }

    @PUT
    @Path(PATH_AUTO_INDEX_STATUS)
    public Response setAutoIndexerEnabled( @PathParam("type") String type, String enable )
    {
        actions.setAutoIndexerEnabled( type, Boolean.parseBoolean( enable ) );
        return output.ok( Representation.emptyRepresentation() );
    }

    @GET
    @Path(PATH_AUTO_INDEXED_PROPERTIES)
    public Response getAutoIndexedProperties( @PathParam("type") String type )
    {
        return output.ok( actions.getAutoIndexedProperties( type ) );
    }

    @POST
    @Path(PATH_AUTO_INDEXED_PROPERTIES)
    public Response startAutoIndexingProperty( @PathParam("type") String type, String property )
    {
        actions.startAutoIndexingProperty( type, property );
        return output.ok( Representation.emptyRepresentation() );

    }

    @DELETE
    @Path(PATH_AUTO_INDEX_PROPERTY_DELETE)
    public Response stopAutoIndexingProperty( @PathParam("type") String type, @PathParam("property") String property )
    {
        actions.stopAutoIndexingProperty( type, property );
        return output.ok( Representation.emptyRepresentation() );
    }

    @GET
    @Path(PATH_NAMED_RELATIONSHIP_INDEX)
    public Response getIndexedRelationshipsByQuery( @PathParam("indexName") String indexName,
                                                    @QueryParam("query") String query,
                                                    @QueryParam("order") String order )
    {
        try
        {
            return output.ok( actions.getIndexedRelationshipsByQuery(
                    indexName, query, order ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX_QUERY_WITH_KEY)
    public Response getIndexedRelationshipsByQuery( @PathParam("indexName") String indexName,
                                                    @PathParam("key") String key,
                                                    @QueryParam("query") String query,
                                                    @QueryParam("order") String order )
    {
        try
        {
            return output.ok( actions.getIndexedRelationshipsByQuery(
                    indexName, key, query, order ) );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_NODE_INDEX_ID)
    public Response deleteFromNodeIndex( @PathParam("indexName") String indexName,
                                         @PathParam("key") String key, @PathParam("value") String value,
                                         @PathParam("id") long id )
    {
        try
        {
            actions.removeFromNodeIndex( indexName, key, value, id );
            return nothing();
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_NODE_INDEX_REMOVE_KEY)
    public Response deleteFromNodeIndexNoValue( @PathParam("indexName") String indexName,
                                                @PathParam("key") String key, @PathParam("id") long id )
    {
        try
        {
            actions.removeFromNodeIndexNoValue( indexName, key, id );
            return nothing();
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_NODE_INDEX_REMOVE)
    public Response deleteFromNodeIndexNoKeyValue( @PathParam("indexName") String indexName, @PathParam("id") long id )
    {
        try
        {
            actions.removeFromNodeIndexNoKeyValue( indexName, id );
            return nothing();
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_INDEX_ID)
    public Response deleteFromRelationshipIndex( @PathParam("indexName") String indexName,
                                                 @PathParam("key") String key, @PathParam("value") String value,
                                                 @PathParam("id") long id )
    {
        try
        {
            actions.removeFromRelationshipIndex( indexName, key, value, id );
            return nothing();
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_INDEX_REMOVE_KEY)
    public Response deleteFromRelationshipIndexNoValue( @PathParam("indexName") String indexName,
                                                        @PathParam("key") String key, @PathParam("id") long id )
    {
        try
        {
            actions.removeFromRelationshipIndexNoValue( indexName, key, id );
            return nothing();
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_INDEX_REMOVE)
    public Response deleteFromRelationshipIndex( @PathParam("indexName") String indexName,
                                                 @PathParam("id") long id )
    {
        try
        {
            actions.removeFromRelationshipIndexNoKeyValue( indexName, id );
            return nothing();
        }
        catch ( UnsupportedOperationException e )
        {
            return output.methodNotAllowed( e );
        }
        catch ( NotFoundException nfe )
        {
            return output.notFound( nfe );
        }
        catch ( Exception e )
        {
            return output.serverError( e );
        }
    }

    // Traversal

    @POST
    @Path(PATH_NODE_TRAVERSE)
    public Response traverse( @PathParam("nodeId") long startNode,
                              @PathParam("returnType") TraverserReturnType returnType, String body )
    {
        try
        {
            return output.ok( actions.traverse( startNode, input.readMap( body ), returnType ) );
        }
        catch ( EvaluationException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( NotFoundException e )
        {
            return output.notFound( e );
        }
    }

    // Paged traversal

    @DELETE
    @Path(PATH_TO_PAGED_TRAVERSERS)
    public Response removePagedTraverser( @PathParam("traverserId") String traverserId )
    {
        if ( actions.removePagedTraverse( traverserId ) )
        {
            return output.ok();
        }
        else
        {
            return output.notFound();
        }
    }

    @GET
    @Path(PATH_TO_PAGED_TRAVERSERS)
    public Response pagedTraverse( @PathParam("traverserId") String traverserId,
                                   @PathParam("returnType") TraverserReturnType returnType )
    {
        try
        {
            return output.ok( actions.pagedTraverse( traverserId, returnType ) );
        }
        catch ( EvaluationException e )
        {
            return output.badRequest( e );
        }
        catch ( NotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @POST
    @Path(PATH_TO_CREATE_PAGED_TRAVERSERS)
    public Response createPagedTraverser( @PathParam("nodeId") long startNode,
                                          @PathParam("returnType") TraverserReturnType returnType,
                                          @QueryParam("pageSize") @DefaultValue(FIFTY_ENTRIES) int pageSize,
                                          @QueryParam("leaseTime") @DefaultValue(SIXTY_SECONDS) int
                                                  leaseTimeInSeconds, String body )
    {
        try
        {
            validatePageSize( pageSize );
            validateLeaseTime( leaseTimeInSeconds );

            String traverserId = actions.createPagedTraverser( startNode, input.readMap( body ), pageSize,
                    leaseTimeInSeconds );

            URI uri = new URI( "node/" + startNode + "/paged/traverse/" + returnType + "/" + traverserId );

            return output.created( new ListEntityRepresentation( actions.pagedTraverse( traverserId, returnType ),
                    uri.normalize() ) );
        }
        catch ( EvaluationException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( NotFoundException e )
        {
            return output.notFound( e );
        }
        catch ( URISyntaxException e )
        {
            return output.serverError( e );
        }
    }

    private void validateLeaseTime( int leaseTimeInSeconds ) throws BadInputException
    {
        if ( leaseTimeInSeconds < 1 )
        {
            throw new InvalidArgumentsException( "Lease time less than 1 second is not supported" );
        }
    }

    private void validatePageSize( int pageSize ) throws BadInputException
    {
        if ( pageSize < 1 )
        {
            throw new InvalidArgumentsException( "Page size less than 1 is not permitted" );
        }
    }

    @POST
    @Path(PATH_NODE_PATH)
    public Response singlePath( @PathParam("nodeId") long startNode, String body )
    {
        final Map<String, Object> description;
        final long endNode;
        try
        {
            description = input.readMap( body );
            endNode = extractNodeId( (String) description.get( "to" ) );
            return output.ok( actions.findSinglePath( startNode, endNode, description ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ClassCastException e )
        {
            return output.badRequest( e );
        }
        catch ( NotFoundException e )
        {
            return output.notFound( e );
        }

    }

    @POST
    @Path(PATH_NODE_PATHS)
    public Response allPaths( @PathParam("nodeId") long startNode, String body )
    {
        final Map<String, Object> description;
        final long endNode;
        try
        {
            description = input.readMap( body );
            endNode = extractNodeId( (String) description.get( "to" ) );
            return output.ok( actions.findPaths( startNode, endNode, description ) );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( ClassCastException e )
        {
            return output.badRequest( e );
        }
    }

    @POST
    @Path( PATH_SCHEMA_INDEX_LABEL )
    public Response createSchemaIndex( @PathParam( "label" ) String labelName, String body )
    {
        try
        {
            Map<String, Object> data = input.readMap( body, "property_keys" );
            Iterable<String> singlePropertyKey = singleOrList( data, "property_keys" );
            if ( singlePropertyKey == null )
            {
                return output.badRequest( new IllegalArgumentException(
                        "Supply single property key or list of property keys" ) );
            }
            return output.ok( actions.createSchemaIndex( labelName, singlePropertyKey ) );
        }
        catch( UnsupportedOperationException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    private Iterable<String> singleOrList( Map<String, Object> data, String key )
    {
        Object propertyKeys = data.get( key );
        Iterable<String> singlePropertyKey = null;
        if ( propertyKeys instanceof List )
        {
            singlePropertyKey = (List<String>) propertyKeys;
        }
        else if ( propertyKeys instanceof String )
        {
            singlePropertyKey = Arrays.asList((String)propertyKeys);
        }
        return singlePropertyKey;
    }

    @DELETE
    @Path( PATH_SCHEMA_INDEX_LABEL_PROPERTY )
    public Response dropSchemaIndex( @PathParam( "label" ) String labelName,
            @PathParam( "property" ) AmpersandSeparatedCollection properties )
    {
        // TODO assumption, only a single property key
        if ( properties.size() != 1 )
        {
            return output.badRequest( new IllegalArgumentException( "Single property key assumed" ) );
        }

        String property = single( properties );
        try
        {
            if ( actions.dropSchemaIndex( labelName, property ) )
            {
                return nothing();
            }
            else
            {
                return output.notFound();
            }
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    @GET
    @Path( PATH_SCHEMA_INDEX )
    public Response getSchemaIndexes()
    {
        return output.ok( actions.getSchemaIndexes() );
    }

    @GET
    @Path( PATH_SCHEMA_INDEX_LABEL )
    public Response getSchemaIndexesForLabel( @PathParam("label") String labelName )
    {
        return output.ok( actions.getSchemaIndexes( labelName ) );
    }

    @POST
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS )
    public Response createPropertyUniquenessConstraint( @PathParam( "label" ) String labelName, String body )
    {
        try
        {
            Map<String, Object> data = input.readMap( body, "property_keys" );
            Iterable<String> singlePropertyKey = singleOrList( data, "property_keys" );
            if ( singlePropertyKey == null )
            {
                return output.badRequest( new IllegalArgumentException(
                        "Supply single property key or list of property keys" ) );
            }
            return output.ok( actions.createPropertyUniquenessConstraint( labelName, singlePropertyKey ) );
        }
        catch( UnsupportedOperationException e )
        {
            return output.badRequest( e );
        }
        catch ( BadInputException e )
        {
            return output.badRequest( e );
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    @DELETE
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS_PROPERTY )
    public Response dropPropertyUniquenessConstraint( @PathParam( "label" ) String labelName,
            @PathParam( "property" ) AmpersandSeparatedCollection properties )
    {
        try
        {
            if ( actions.dropPropertyUniquenessConstraint( labelName, properties ) )
            {
                return nothing();
            }
            else
            {
                return output.notFound();
            }
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    @DELETE
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_EXISTENCE_PROPERTY )
    public Response dropNodePropertyExistenceConstraint( @PathParam( "label" ) String labelName,
            @PathParam( "property" ) AmpersandSeparatedCollection properties )
    {
        try
        {
            if ( actions.dropNodePropertyExistenceConstraint( labelName, properties ) )
            {
                return nothing();
            }
            else
            {
                return output.notFound();
            }
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    @DELETE
    @Path( PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_EXISTENCE_PROPERTY )
    public Response dropRelationshipPropertyExistenceConstraint( @PathParam( "type" ) String typeName,
            @PathParam( "property" ) AmpersandSeparatedCollection properties )
    {
        try
        {
            if ( actions.dropRelationshipPropertyExistenceConstraint( typeName, properties ) )
            {
                return nothing();
            }
            else
            {
                return output.notFound();
            }
        }
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
    }

    @GET
    @Path(PATH_SCHEMA_CONSTRAINT)
    public Response getSchemaConstraints()
    {
        return output.ok( actions.getConstraints() );
    }

    @GET
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL )
    public Response getSchemaConstraintsForLabel( @PathParam( "label" ) String labelName )
    {
        return output.ok( actions.getLabelConstraints( labelName ) );
    }

    @GET
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS )
    public Response getSchemaConstraintsForLabelAndUniqueness( @PathParam( "label" ) String labelName )
    {
        return output.ok( actions.getLabelUniquenessConstraints( labelName ) );
    }

    @GET
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_EXISTENCE )
    public Response getSchemaConstraintsForLabelAndExistence( @PathParam( "label" ) String labelName )
    {
        return output.ok( actions.getLabelExistenceConstraints( labelName ) );
    }

    @GET
    @Path( PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_TYPE_EXISTENCE )
    public Response getSchemaConstraintsForRelationshipTypeAndExistence( @PathParam( "type" ) String typeName )
    {
        return output.ok( actions.getRelationshipTypeExistenceConstraints( typeName ) );
    }

    @GET
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_UNIQUENESS_PROPERTY )
    public Response getSchemaConstraintsForLabelAndPropertyUniqueness( @PathParam( "label" ) String labelName,
            @PathParam( "property" ) AmpersandSeparatedCollection propertyKeys )
    {
        try
        {
        	ListRepresentation constraints = actions.getPropertyUniquenessConstraint( labelName, propertyKeys );
    		return output.ok( constraints );
        }
        catch ( IllegalArgumentException e )
        {
        	return output.notFound( e );
        }
    }

    @GET
    @Path( PATH_SCHEMA_CONSTRAINT_LABEL_EXISTENCE_PROPERTY )
    public Response getSchemaConstraintsForLabelAndPropertyExistence( @PathParam( "label" ) String labelName,
            @PathParam( "property" ) AmpersandSeparatedCollection propertyKeys )
    {
        try
        {
            ListRepresentation constraints = actions.getNodePropertyExistenceConstraint( labelName, propertyKeys );
            return output.ok( constraints );
        }
        catch ( IllegalArgumentException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path( PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_EXISTENCE_PROPERTY )
    public Response getSchemaConstraintsForRelationshipTypeAndPropertyExistence( @PathParam( "type" ) String typeName,
            @PathParam( "property" ) AmpersandSeparatedCollection propertyKeys )
    {
        try
        {
            ListRepresentation constraints = actions.getRelationshipPropertyExistenceConstraint( typeName, propertyKeys );
            return output.ok( constraints );
        }
        catch ( IllegalArgumentException e )
        {
            return output.notFound( e );
        }
    }

    private final Function<Map.Entry<String,List<String>>,Pair<String,Object>> queryParamsToProperties =
            new Function<Map.Entry<String, List<String>>, Pair<String, Object>>()
    {
        @Override
        public Pair<String, Object> apply( Map.Entry<String, List<String>> queryEntry )
        {
            try
            {
                Object propertyValue = input.readValue( queryEntry.getValue().get( 0 ) );
                if ( propertyValue instanceof Collection<?> )
                {
                    propertyValue = PropertySettingStrategy.convertToNativeArray( (Collection<?>) propertyValue );
                }
                return Pair.of( queryEntry.getKey(), propertyValue );
            }
            catch ( BadInputException e )
            {
                throw new IllegalArgumentException(
                        String.format( "Unable to deserialize property value for %s.", queryEntry.getKey() ),
                        e );
            }
        }
    };
}
