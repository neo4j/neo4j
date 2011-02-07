/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.web;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.StartNodeSameAsEndNodeException;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.PropertiesRepresentation;
import org.neo4j.server.rest.web.DatabaseActions.RelationshipDirection;

@Path("/")
public class RestfulGraphDatabase {
    @SuppressWarnings("serial")
    public static class AmpersandSeparatedCollection extends LinkedHashSet<String> {
        public AmpersandSeparatedCollection(String path) {
            for (String e : path.split("&")) {
                if (e.trim().length() > 0) {
                    add(e);
                }
            }
        }
    }

    private static final String PATH_NODES = "node";
    private static final String PATH_NODE = PATH_NODES + "/{nodeId}";
    private static final String PATH_NODE_PROPERTIES = PATH_NODE + "/properties";
    private static final String PATH_NODE_PROPERTY = PATH_NODE_PROPERTIES + "/{key}";
    private static final String PATH_NODE_RELATIONSHIPS = PATH_NODE + "/relationships";
    private static final String PATH_RELATIONSHIP = "relationship/{relationshipId}";
    private static final String PATH_NODE_RELATIONSHIPS_W_DIR = PATH_NODE_RELATIONSHIPS + "/{direction}";
    private static final String PATH_NODE_RELATIONSHIPS_W_DIR_N_TYPES = PATH_NODE_RELATIONSHIPS_W_DIR + "/{types}";
    private static final String PATH_RELATIONSHIP_PROPERTIES = PATH_RELATIONSHIP + "/properties";
    private static final String PATH_RELATIONSHIP_PROPERTY = PATH_RELATIONSHIP_PROPERTIES + "/{key}";
    private static final String PATH_NODE_TRAVERSE = PATH_NODE + "/traverse/{returnType}";
    private static final String PATH_NODE_PATH = PATH_NODE + "/path";
    private static final String PATH_NODE_PATHS = PATH_NODE + "/paths";

    protected static final String PATH_NODE_INDEX = "index/node";
    protected static final String PATH_NAMED_NODE_INDEX = PATH_NODE_INDEX + "/{indexName}";
    protected static final String PATH_NODE_INDEX_QUERY = PATH_NODE_INDEX + "/{indexName}/{key}/{value}";
    protected static final String PATH_NODE_INDEX_ID = PATH_NODE_INDEX_QUERY + "/{id}";

    protected static final String PATH_RELATIONSHIP_INDEX = "index/relationship";
    protected static final String PATH_NAMED_RELATIONSHIP_INDEX = PATH_RELATIONSHIP_INDEX + "/{indexName}";
    protected static final String PATH_RELATIONSHIP_INDEX_QUERY = PATH_RELATIONSHIP_INDEX + "/{indexName}/{key}/{value}";
    protected static final String PATH_RELATIONSHIP_INDEX_ID = PATH_RELATIONSHIP_INDEX_QUERY + "/{id}";

    private final DatabaseActions server;
    private final OutputFormat output;
    private final InputFormat input;

    public RestfulGraphDatabase(@Context UriInfo uriInfo, @Context Database database, @Context InputFormat input, @Context OutputFormat output) {
        this.input = input;
        this.output = output;
        this.server = new DatabaseActions(database);
    }

    private static Response nothing() {
        return Response.noContent().build();
    }

    private long extractNodeId(String uri) throws BadInputException {
        try {
            return Long.parseLong(uri.substring(uri.lastIndexOf("/") + 1));
        } catch (NumberFormatException ex) {
            throw new BadInputException(ex);
        } catch (NullPointerException ex) {
            throw new BadInputException(ex);
        }
    }

    @GET
    public Response getRoot() {
        return output.ok(server.root());
    }

    // Nodes

    @POST
    @Path(PATH_NODES)
    public Response createNode(String body) {
        try {
            return output.created(server.createNode(input.readMap(body)));
        } catch (ArrayStoreException ase) {
            return Response.status(400).type(MediaType.TEXT_PLAIN).entity("Invalid JSON array in POST body: " + body).build();
        } catch (BadInputException e) {
            return output.badRequest(e);
        }
    }

    @GET
    @Path(PATH_NODE)
    public Response getNode(@PathParam("nodeId") long nodeId) {
        try {
            return output.ok(server.getNode(nodeId));
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }
    }

    @DELETE
    @Path(PATH_NODE)
    public Response deleteNode(@PathParam("nodeId") long nodeId) {
        try {
            server.deleteNode(nodeId);
            return nothing();
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        } catch (OperationFailureException e) {
            return output.conflict(e);
        }
    }

    // Node properties

    @PUT
    @Path(PATH_NODE_PROPERTIES)
    public Response setAllNodeProperties(@PathParam("nodeId") long nodeId, String body) {
        try {
            server.setAllNodeProperties(nodeId, input.readMap(body));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @GET
    @Path(PATH_NODE_PROPERTIES)
    public Response getAllNodeProperties(@PathParam("nodeId") long nodeId) {
        final PropertiesRepresentation properties;
        try {
            properties = server.getAllNodeProperties(nodeId);
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }

        if (properties.isEmpty()) {
            return nothing();
        }

        return output.ok(properties);
    }

    @PUT
    @Path(PATH_NODE_PROPERTY)
    public Response setNodeProperty(@PathParam("nodeId") long nodeId, @PathParam("key") String key, String body) {
        try {
            server.setNodeProperty(nodeId, key, input.readValue(body));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @GET
    @Path(PATH_NODE_PROPERTY)
    public Response getNodeProperty(@PathParam("nodeId") long nodeId, @PathParam("key") String key) {
        try {
            return output.ok(server.getNodeProperty(nodeId, key));
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        } catch (NoSuchPropertyException e) {
            return output.notFound(e);
        }
    }

    @DELETE
    @Path(PATH_NODE_PROPERTY)
    public Response deleteNodeProperty(@PathParam("nodeId") long nodeId, @PathParam("key") String key) {
        try {
            server.removeNodeProperty(nodeId, key);
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        } catch (NoSuchPropertyException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @DELETE
    @Path(PATH_NODE_PROPERTIES)
    public Response deleteAllNodeProperties(@PathParam("nodeId") long nodeId) {
        try {
            server.removeAllNodeProperties(nodeId);
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    // Relationships

    @POST
    @Path(PATH_NODE_RELATIONSHIPS)
    public Response createRelationship(@PathParam("nodeId") long startNodeId, String body) {
        final Map<String, Object> data;
        final long endNodeId;
        final String type;
        final Map<String, Object> properties;
        try {
            data = input.readMap(body);
            endNodeId = extractNodeId((String) data.get("to"));
            type = (String) data.get("type");
            properties = (Map<String, Object>) data.get("data");
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (ClassCastException e) {
            return output.badRequest(e);
        }
        try {
            return output.created(server.createRelationship(startNodeId, endNodeId, type, properties));
        } catch (StartNodeNotFoundException e) {
            return output.notFound(e);
        } catch (EndNodeNotFoundException e) {
            return output.badRequest(e);
        } catch (StartNodeSameAsEndNodeException e) {
            return output.badRequest(e);
        } catch (PropertyValueException e) {
            return output.badRequest(e);
        } catch (BadInputException e) {
            return output.badRequest(e);
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP)
    public Response getRelationship(@PathParam("relationshipId") long relationshipId) {
        try {
            return output.ok(server.getRelationship(relationshipId));
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        }
    }

    @DELETE
    @Path(PATH_RELATIONSHIP)
    public Response deleteRelationship(@PathParam("relationshipId") long relationshipId) {
        try {
            server.deleteRelationship(relationshipId);
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @GET
    @Path(PATH_NODE_RELATIONSHIPS_W_DIR)
    public Response getNodeRelationships(@PathParam("nodeId") long nodeId, @PathParam("direction") RelationshipDirection direction) {
        try {
            return output.ok(server.getNodeRelationships(nodeId, direction, Collections.<String> emptyList()));
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }
    }

    @GET
    @Path(PATH_NODE_RELATIONSHIPS_W_DIR_N_TYPES)
    public Response getNodeRelationships(@PathParam("nodeId") long nodeId, @PathParam("direction") RelationshipDirection direction,
            @PathParam("types") AmpersandSeparatedCollection types) {
        try {
            return output.ok(server.getNodeRelationships(nodeId, direction, types));
        } catch (NodeNotFoundException e) {
            return output.notFound(e);
        }
    }

    // Relationship properties

    @GET
    @Path(PATH_RELATIONSHIP_PROPERTIES)
    public Response getAllRelationshipProperties(@PathParam("relationshipId") long relationshipId) {
        final PropertiesRepresentation properties;
        try {
            properties = server.getAllRelationshipProperties(relationshipId);
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        }
        if (properties.isEmpty()) {
            return nothing();
        } else {
            return output.ok(properties);
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_PROPERTY)
    public Response getRelationshipProperty(@PathParam("relationshipId") long relationshipId, @PathParam("key") String key) {
        try {
            return output.ok(server.getRelationshipProperty(relationshipId, key));
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        } catch (NoSuchPropertyException e) {
            return output.notFound(e);
        }
    }

    @PUT
    @Path(PATH_RELATIONSHIP_PROPERTIES)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setAllRelationshipProperties(@PathParam("relationshipId") long relationshipId, String body) {
        try {
            server.setAllRelationshipProperties(relationshipId, input.readMap(body));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @PUT
    @Path(PATH_RELATIONSHIP_PROPERTY)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response setRelationshipProperty(@PathParam("relationshipId") long relationshipId, @PathParam("key") String key, String body) {
        try {
            server.setRelationshipProperty(relationshipId, key, input.readValue(body));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_PROPERTIES)
    public Response deleteAllRelationshipProperties(@PathParam("relationshipId") long relationshipId) {
        try {
            server.removeAllRelationshipProperties(relationshipId);
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_PROPERTY)
    public Response deleteRelationshipProperty(@PathParam("relationshipId") long relationshipId, @PathParam("key") String key) {
        try {
            server.removeRelationshipProperty(relationshipId, key);
        } catch (RelationshipNotFoundException e) {
            return output.notFound(e);
        } catch (NoSuchPropertyException e) {
            return output.notFound(e);
        }
        return nothing();
    }

    // Index

    @GET
    @Path(PATH_NODE_INDEX)
    public Response getNodeIndexRoot() {
        if (server.getNodeIndexNames().length == 0) {
            return output.noContent();
        }
        return output.ok(server.nodeIndexRoot());
    }

    @POST
    @Path(PATH_NODE_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response jsonCreateNodeIndex(String json) {
        try {
            return output.created(server.createNodeIndex(input.readMap(json)));
        } catch (BadInputException e) {
            return output.badRequest(e);
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX)
    public Response getRelationshipIndexRoot() {
        if (server.getRelationshipIndexNames().length == 0) {
            return output.noContent();
        }
        return output.ok(server.relationshipIndexRoot());
    }

    @POST
    @Path(PATH_RELATIONSHIP_INDEX)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response jsonCreateRelationshipIndex(String json) {
        try {
            return output.created(server.createRelationshipIndex(input.readMap(json)));
        } catch (BadInputException e) {
            return output.badRequest(e);
        }
    }

    @POST
    @Path(PATH_NODE_INDEX_QUERY)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addToNodeIndex(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value, String objectUri) {
        try {
            return output.created(server.addToNodeIndex(indexName, key, value, extractNodeId(input.readUri(objectUri).toString())));
        } catch (BadInputException e) {
            return output.badRequest(e);
        }
    }

    @POST
    @Path(PATH_RELATIONSHIP_INDEX_QUERY)
    public Response addToRelationshipIndex(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value,
            String objectUri) {
        try {
            return output.created(server.addToRelationshipIndex(indexName, key, value, extractNodeId(input.readUri(objectUri).toString())));
        } catch (BadInputException e) {
            return output.badRequest(e);
        }
    }

    @GET
    @Path(PATH_NODE_INDEX_ID)
    public Response getNodeFromIndexUri(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value,
            @PathParam("id") long id) {
        try {
            return output.ok(server.getIndexedNode(indexName, key, value, id));
        } catch (NotFoundException nfe) {
            return output.notFound(nfe);
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX_ID)
    public Response getRelationshipFromIndexUri(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value,
            @PathParam("id") long id) {
        return output.ok(server.getIndexedRelationship(indexName, key, value, id));
    }

    @GET
    @Path(PATH_NODE_INDEX_QUERY)
    public Response getIndexedNodes(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value) {
        try {
            return output.ok(server.getIndexedNodesByExactMatch(indexName, key, value));
        } catch (NotFoundException nfe) {
            return output.notFound(nfe);
        }
    }

    @GET
    @Path(PATH_RELATIONSHIP_INDEX_QUERY)
    public Response getIndexedRelationships(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value) {
        try {
            return output.ok(server.getIndexedRelationships(indexName, key, value));
        } catch (NotFoundException nfe) {
            return output.notFound(nfe);
        }
    }

    @DELETE
    @Path(PATH_NODE_INDEX_ID)
    public Response deleteFromNodeIndex(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value,
            @PathParam("id") long id) {
        try {
            server.removeFromNodeIndex(indexName, key, value, id);
            return nothing();
        } catch (NotFoundException nfe) {
            return output.notFound(nfe);
        }
    }

    @DELETE
    @Path(PATH_RELATIONSHIP_INDEX_ID)
    public Response deleteFromRelationshipIndex(@PathParam("indexName") String indexName, @PathParam("key") String key, @PathParam("value") String value,
            @PathParam("id") long id) {
        try {
            server.removeFromRelationshipIndex(indexName, key, value, id);
            return nothing();
        } catch (NotFoundException nfe) {
            return output.notFound(nfe);
        }

    }

    // Traversal

    @POST
    @Path(PATH_NODE_TRAVERSE)
    public Response traverse(@PathParam("nodeId") long startNode, @PathParam("returnType") TraverserReturnType returnType, String body) {
        try {
            return output.ok(server.traverse(startNode, input.readMap(body), returnType));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (NotFoundException e) {
            return output.notFound(e);
        }
    }

    @POST
    @Path(PATH_NODE_PATH)
    public Response singlePath(@PathParam("nodeId") long startNode, String body) {
        final Map<String, Object> description;
        final long endNode;
        try {
            description = input.readMap(body);
            endNode = extractNodeId((String) description.get("to"));
            return output.ok(server.findSinglePath(startNode, endNode, description));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (ClassCastException e) {
            return output.badRequest(e);
        } catch (NotFoundException e) {
            return output.notFound();
        }

    }

    @POST
    @Path(PATH_NODE_PATHS)
    public Response allPaths(@PathParam("nodeId") long startNode, String body) {
        final Map<String, Object> description;
        final long endNode;
        try {
            description = input.readMap(body);
            endNode = extractNodeId((String) description.get("to"));
            return output.ok(server.findPaths(startNode, endNode, description));
        } catch (BadInputException e) {
            return output.badRequest(e);
        } catch (ClassCastException e) {
            return output.badRequest(e);
        }
    }

}
