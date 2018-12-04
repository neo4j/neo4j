/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.PropertySettingStrategy;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.InvalidArgumentsException;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.MapUtil.toMap;
import static org.neo4j.server.rest.web.Surface.PATH_LABELS;
import static org.neo4j.server.rest.web.Surface.PATH_NODES;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIPS;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_CONSTRAINT;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_RELATIONSHIP_CONSTRAINT;

@Path( "/" )
public class RestfulGraphDatabase
{
    @SuppressWarnings( "serial" )
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
    private static final String PATH_NODE_PATH = PATH_NODE + "/path";
    private static final String PATH_NODE_PATHS = PATH_NODE + "/paths";
    private static final String PATH_NODE_LABELS = PATH_NODE + "/labels";
    private static final String PATH_NODE_LABEL = PATH_NODE + "/labels/{label}";
    private static final String PATH_NODE_DEGREE = PATH_NODE + "/degree";
    private static final String PATH_NODE_DEGREE_W_DIR = PATH_NODE_DEGREE + "/{direction}";
    private static final String PATH_NODE_DEGREE_W_DIR_N_TYPES = PATH_NODE_DEGREE_W_DIR + "/{types}";

    private static final String PATH_PROPERTY_KEYS = "propertykeys";

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
    public static final String PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_EXISTENCE_PROPERTY =
            PATH_SCHEMA_RELATIONSHIP_CONSTRAINT_TYPE_EXISTENCE + "/{property}";

    public static final String NODE_AUTO_INDEX_TYPE = "node";
    public static final String RELATIONSHIP_AUTO_INDEX_TYPE = "relationship";

    private final DatabaseActions actions;
    private Configuration config;
    private final OutputFormat output;
    private final InputFormat input;

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

    private long extractNodeId( String uri ) throws BadInputException
    {
        try
        {
            return Long.parseLong( uri.substring( uri.lastIndexOf( '/' ) + 1 ) );
        }
        catch ( NumberFormatException | NullPointerException ex )
        {
            throw new BadInputException( ex );
        }
    }

    @GET
    public Response getRoot()
    {
        return output.ok( actions.root() );
    }

    // Nodes

    @POST
    @Path( PATH_NODES )
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
        catch ( BadInputException | ClassCastException e )
        {
            return output.badRequest( e );
        }
    }

    private Response generateBadRequestDueToMangledJsonResponse( String body )
    {
        return output.badRequest( MediaType.TEXT_PLAIN_TYPE, "Invalid JSON array in POST body: " + body );
    }

    @GET
    @Path( PATH_NODE )
    public Response getNode( @PathParam( "nodeId" ) long nodeId )
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
    @Path( PATH_NODE )
    public Response deleteNode( @PathParam( "nodeId" ) long nodeId )
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
    @Path( PATH_NODE_PROPERTIES )
    public Response setAllNodeProperties( @PathParam( "nodeId" ) long nodeId, String body )
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
    @Path( PATH_NODE_PROPERTIES )
    public Response getAllNodeProperties( @PathParam( "nodeId" ) long nodeId )
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
    @Path( PATH_NODE_PROPERTY )
    public Response setNodeProperty( @PathParam( "nodeId" ) long nodeId,
                                     @PathParam( "key" ) String key, String body )
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
        catch ( org.neo4j.graphdb.ConstraintViolationException e )
        {
            return output.conflict( e );
        }
        return nothing();
    }

    @GET
    @Path( PATH_NODE_PROPERTY )
    public Response getNodeProperty( @PathParam( "nodeId" ) long nodeId, @PathParam( "key" ) String key )
    {
        try
        {
            return output.ok( actions.getNodeProperty( nodeId, key ) );
        }
        catch ( NodeNotFoundException | NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
    }

    @DELETE
    @Path( PATH_NODE_PROPERTY )
    public Response deleteNodeProperty( @PathParam( "nodeId" ) long nodeId, @PathParam( "key" ) String key )
    {
        try
        {
            actions.removeNodeProperty( nodeId, key );
        }
        catch ( NodeNotFoundException | NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @DELETE
    @Path( PATH_NODE_PROPERTIES )
    public Response deleteAllNodeProperties( @PathParam( "nodeId" ) long nodeId )
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
            else if ( rawInput instanceof Collection )
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
    public Response getNodesWithLabelAndProperty( @PathParam( "label" ) String labelName, @Context UriInfo uriInfo )
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

    @SuppressWarnings( "unchecked" )
    @POST
    @Path( PATH_NODE_RELATIONSHIPS )
    public Response createRelationship( @PathParam( "nodeId" ) long startNodeId, String body )
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
        catch ( BadInputException | ClassCastException e )
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
        catch ( EndNodeNotFoundException | BadInputException e )
        {
            return output.badRequest( e );
        }
    }

    @GET
    @Path( PATH_RELATIONSHIP )
    public Response getRelationship( @PathParam( "relationshipId" ) long relationshipId )
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
    @Path( PATH_RELATIONSHIP )
    public Response deleteRelationship( @PathParam( "relationshipId" ) long relationshipId )
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
    @Path( PATH_NODE_RELATIONSHIPS_W_DIR )
    public Response getNodeRelationships( @PathParam( "nodeId" ) long nodeId,
                                          @PathParam( "direction" ) RelationshipDirection direction )
    {
        try
        {
            return output.ok( actions.getNodeRelationships( nodeId, direction, Collections.emptyList() ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path( PATH_NODE_RELATIONSHIPS_W_DIR_N_TYPES )
    public Response getNodeRelationships( @PathParam( "nodeId" ) long nodeId,
                                          @PathParam( "direction" ) RelationshipDirection direction,
                                          @PathParam( "types" ) AmpersandSeparatedCollection types )
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
    @Path( PATH_NODE_DEGREE_W_DIR )
    public Response getNodeDegree( @PathParam( "nodeId" ) long nodeId,
                                   @PathParam( "direction" ) RelationshipDirection direction )
    {
        try
        {
            return output.ok( actions.getNodeDegree( nodeId, direction, Collections.emptyList() ) );
        }
        catch ( NodeNotFoundException e )
        {
            return output.notFound( e );
        }
    }

    @GET
    @Path( PATH_NODE_DEGREE_W_DIR_N_TYPES )
    public Response getNodeDegree( @PathParam( "nodeId" ) long nodeId,
                                  @PathParam( "direction" ) RelationshipDirection direction,
                                  @PathParam( "types" ) AmpersandSeparatedCollection types )
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
    @Path( PATH_RELATIONSHIP_PROPERTIES )
    public Response getAllRelationshipProperties( @PathParam( "relationshipId" ) long relationshipId )
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
    @Path( PATH_RELATIONSHIP_PROPERTY )
    public Response getRelationshipProperty( @PathParam( "relationshipId" ) long relationshipId,
                                             @PathParam( "key" ) String key )
    {
        try
        {
            return output.ok( actions.getRelationshipProperty( relationshipId, key ) );
        }
        catch ( RelationshipNotFoundException | NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
    }

    @PUT
    @Path( PATH_RELATIONSHIP_PROPERTIES )
    @Consumes( MediaType.APPLICATION_JSON )
    public Response setAllRelationshipProperties( @PathParam( "relationshipId" ) long relationshipId, String body )
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
    @Path( PATH_RELATIONSHIP_PROPERTY )
    @Consumes( MediaType.APPLICATION_JSON )
    public Response setRelationshipProperty( @PathParam( "relationshipId" ) long relationshipId,
                                             @PathParam( "key" ) String key, String body )
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
    @Path( PATH_RELATIONSHIP_PROPERTIES )
    public Response deleteAllRelationshipProperties( @PathParam( "relationshipId" ) long relationshipId )
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
    @Path( PATH_RELATIONSHIP_PROPERTY )
    public Response deleteRelationshipProperty( @PathParam( "relationshipId" ) long relationshipId,
                                                @PathParam( "key" ) String key )
    {
        try
        {
            actions.removeRelationshipProperty( relationshipId, key );
        }
        catch ( RelationshipNotFoundException | NoSuchPropertyException e )
        {
            return output.notFound( e );
        }
        return nothing();
    }

    @POST
    @Path( PATH_NODE_PATH )
    public Response singlePath( @PathParam( "nodeId" ) long startNode, String body )
    {
        final Map<String, Object> description;
        final long endNode;
        try
        {
            description = input.readMap( body );
            endNode = extractNodeId( (String) description.get( "to" ) );
            return output.ok( actions.findSinglePath( startNode, endNode, description ) );
        }
        catch ( BadInputException | ClassCastException e )
        {
            return output.badRequest( e );
        }
        catch ( NotFoundException e )
        {
            return output.notFound( e );
        }

    }

    @POST
    @Path( PATH_NODE_PATHS )
    public Response allPaths( @PathParam( "nodeId" ) long startNode, String body )
    {
        final Map<String, Object> description;
        final long endNode;
        try
        {
            description = input.readMap( body );
            endNode = extractNodeId( (String) description.get( "to" ) );
            return output.ok( actions.findPaths( startNode, endNode, description ) );
        }
        catch ( BadInputException | ClassCastException e )
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
        catch ( UnsupportedOperationException | BadInputException e )
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
            singlePropertyKey = Collections.singletonList( (String) propertyKeys );
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

        String property = Iterables.single( properties );
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
    public Response getSchemaIndexesForLabel( @PathParam( "label" ) String labelName )
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
        catch ( UnsupportedOperationException | BadInputException e )
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
    @Path( PATH_SCHEMA_CONSTRAINT )
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
