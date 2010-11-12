/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.AmpersandSeparatedList;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.EvaluationException;
import org.neo4j.server.rest.domain.IndexRootRepresentation;
import org.neo4j.server.rest.domain.IndexedNodeRepresentation;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.NodeRepresentation;
import org.neo4j.server.rest.domain.PathRepresentation;
import org.neo4j.server.rest.domain.PropertiesMap;
import org.neo4j.server.rest.domain.PropertyValueException;
import org.neo4j.server.rest.domain.RelationshipDirection;
import org.neo4j.server.rest.domain.RelationshipRepresentation;
import org.neo4j.server.rest.domain.Representation;
import org.neo4j.server.rest.domain.RootRepresentation;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.StartNodeSameAsEndNodeException;
import org.neo4j.server.rest.domain.StorageActions;
import org.neo4j.server.rest.domain.StorageActions.TraverserReturnType;
import org.neo4j.server.rest.domain.renderers.Renderer;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class GenericWebService {
    public static final String UTF8 = "UTF-8";
    protected static final String PATH_NODES = "node";
    protected static final String PATH_NODE = PATH_NODES + "/{nodeId}";
    protected static final String PATH_NODE_PROPERTIES = PATH_NODE + "/properties";
    protected static final String PATH_NODE_PROPERTY = PATH_NODE_PROPERTIES + "/{key}";
    protected static final String PATH_NODE_RELATIONSHIPS = PATH_NODE + "/relationships";
    protected static final String PATH_NODE_RELATIONSHIPS_W_DIR = PATH_NODE_RELATIONSHIPS + "/{direction}";
    protected static final String PATH_NODE_RELATIONSHIPS_W_DIR_N_TYPES = PATH_NODE_RELATIONSHIPS_W_DIR + "/{types}";
    protected static final String PATH_NODE_TRAVERSE = PATH_NODE + "/traverse/{returnType}";

    protected static final String PATH_NODE_PATH = PATH_NODE + "/path";
    protected static final String PATH_NODE_PATHS = PATH_NODE + "/paths";

    protected static final String PATH_RELATIONSHIP = "relationship/{relationshipId}";
    protected static final String PATH_RELATIONSHIP_PROPERTIES = PATH_RELATIONSHIP + "/properties";
    protected static final String PATH_RELATIONSHIP_PROPERTY = PATH_RELATIONSHIP_PROPERTIES + "/{key}";

    protected static final String PATH_INDEX = "index";
    protected static final String PATH_INDEX_QUERY = PATH_INDEX + "/{indexName}/{key}/{value}";
    protected static final String PATH_INDEX_ID = PATH_INDEX_QUERY + "/{id}";

    protected StorageActions actions;
    private Database database;
    private UriInfo uriInfo;

    GenericWebService(UriInfo uriInfo, Database db) {
        this.uriInfo = uriInfo;
        this.database = db;
        try {
            this.actions = new StorageActions(uriInfo.getBaseUri(), db);
        } catch (DatabaseBlockedException e) {
            throw new RuntimeException("Unable to create GenericWebService, database access is blocked.", e);
        }
    }

    private static String dodgeStartingUnicodeMarker(String string) {
        if (string != null && string.length() > 0) {
            if (string.charAt(0) == 0xfeff) {
                return string.substring(1);
            }
        }
        return string;
    }

    private static ResponseBuilder addHeaders(ResponseBuilder builder) {
        String entity = (String) builder.clone().build().getEntity();
        byte[] entityAsBytes;
        try {
            entityAsBytes = entity.getBytes(UTF8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Could not encode string as UTF-8", e);
        }
        builder = builder.entity(entityAsBytes);
        builder = builder.header(HttpHeaders.CONTENT_LENGTH, String.valueOf(entityAsBytes.length));
        builder = builder.header(HttpHeaders.CONTENT_ENCODING, UTF8);
        return builder;
    }

    protected Response getRoot(Renderer renderer) {
        String entity = renderer.render(new RootRepresentation(uriInfo.getBaseUri(), database));
        return addHeaders(Response.ok(entity, renderer.getMediaType())).build();
    }

    protected Response createEmptyNode(String body, Renderer renderer)

    {
        if (!isNullOrEmpty(body)) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        NodeRepresentation noderep = actions.createNode(new PropertiesMap(Collections.<String, Object> emptyMap()));
        return addHeaders(Response.created(noderep.selfUri()).entity(renderer.render(noderep)).type(renderer.getMediaType())).build();
    }

    protected boolean isNullOrEmpty(String str) {
        return str == null || str.equals("");
    }

    protected Response createNode(String json, Renderer renderer)

    {
        json = dodgeStartingUnicodeMarker(json);
        PropertiesMap properties;
        try {
            properties = new PropertiesMap(JsonHelper.jsonToMap(json));
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        }
        NodeRepresentation noderep = actions.createNode(properties);
        String entity = renderer.render(noderep);
        return addHeaders(Response.created(noderep.selfUri()).entity(entity).type(renderer.getMediaType())).build();
    }

    protected Response buildExceptionResponse(Status status, String message, Exception e, Renderer renderer) {
        return Response.status(status).entity(renderer.renderException(message, e)).build();
    }

    protected Response buildBadJsonExceptionResponse(String json, Exception e, Renderer renderer) {
        return buildExceptionResponse(Status.BAD_REQUEST, "\n----\n" + json + "\n----", e, renderer);
    }

    protected Response getNode(long nodeId, Renderer renderer)

    {
        NodeRepresentation noderep;
        try {
            noderep = actions.retrieveNode(nodeId);
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return addHeaders(Response.ok(renderer.render(noderep), renderer.getMediaType())).build();
    }

    protected Response setNodeProperties(long nodeId, String json, Renderer renderer) {
        json = dodgeStartingUnicodeMarker(json);
        PropertiesMap properties;
        try {
            properties = new PropertiesMap(JsonHelper.jsonToMap(json));
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        }
        try {
            actions.setNodeProperties(nodeId, properties);
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.noContent().build();
    }

    protected Response getNodeProperties(long nodeId, Renderer renderer)

    {
        try {
            PropertiesMap properties = actions.getNodeProperties(nodeId);
            if (properties.isEmpty()) {
                return Response.noContent().build();
            }
            return addHeaders(Response.ok(renderer.render(properties), renderer.getMediaType())).build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response deleteNode(long id, Renderer renderer)

    {
        try {
            actions.deleteNode(id);
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        } catch (TransactionFailureException e) {
            return Response.status(Status.CONFLICT).build();
        }
        return Response.noContent().build();
    }

    protected Response setNodeProperty(long id, String key, String json, Renderer renderer) {
        json = dodgeStartingUnicodeMarker(json);
        try {
            actions.setNodeProperty(id, key, JsonHelper.jsonToSingleValue(json));
            return Response.noContent().build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        }
    }

    protected Response getNodeProperty(long nodeId, String key, Renderer renderer) {
        try {
            Object value = actions.getNodeProperty(nodeId, key);
            return addHeaders(Response.ok(JsonHelper.createJsonFrom(value), renderer.getMediaType())).build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response removeNodeProperties(long nodeId, Renderer renderer)

    {
        try {
            actions.removeNodeProperties(nodeId);
            return Response.noContent().build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response createRelationship(long startNodeId, String json, Renderer renderer) {
        json = dodgeStartingUnicodeMarker(json);
        long endNodeId;
        String type;
        PropertiesMap properties;
        try {
            Map<String, Object> payload = JsonHelper.jsonToMap(json);
            endNodeId = getNodeIdFromUri((String) payload.get("to"));
            type = ((String) payload.get("type")).toString();
            @SuppressWarnings("unchecked")
            Map<String, Object> props = (Map<String, Object>) payload.get("data");
            if (props != null) {
                properties = new PropertiesMap(props);
            } else {
                properties = new PropertiesMap(Collections.<String, Object> emptyMap());
            }
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        }
        RelationshipRepresentation relationship;
        try {
            relationship = actions.createRelationship(type, startNodeId, endNodeId, properties);
        } catch (StartNodeNotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        } catch (EndNodeNotFoundException e) {
            return Response.status(Status.BAD_REQUEST).build();
        } catch (StartNodeSameAsEndNodeException e) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        return addHeaders(Response.created(relationship.selfUri()).entity(renderer.render(relationship)).type(renderer.getMediaType())).build();
    }

    protected Response removeNodeProperty(long nodeId, String key, Renderer renderer) {
        try {
            boolean removed = actions.removeNodeProperty(nodeId, key);
            return removed ? Response.noContent().build() : Response.status(Status.NOT_FOUND).build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response getRelationship(long relationshipId, Renderer renderer)

    {
        RelationshipRepresentation relrep;
        try {
            relrep = actions.retrieveRelationship(relationshipId);
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return addHeaders(Response.ok(renderer.render(relrep), renderer.getMediaType())).build();
    }

    protected Response getRelationshipProperties(long relationshipId, Renderer renderer) {
        try {
            PropertiesMap properties = actions.getRelationshipProperties(relationshipId);
            if (properties.isEmpty()) {
                return Response.noContent().build();
            }
            return addHeaders(Response.ok(renderer.render(properties), renderer.getMediaType())).build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response getRelationshipProperty(long relationshipId, String key, Renderer renderer) {
        try {
            Object value = actions.getRelationshipProperty(relationshipId, key);
            return addHeaders(Response.ok(JsonHelper.createJsonFrom(value), MediaType.APPLICATION_JSON_TYPE)).build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    private long getNodeIdFromUri(String uri) {
        return Long.parseLong(uri.substring(uri.lastIndexOf("/") + 1));
    }

    protected Response removeRelationship(long relationshipId, Renderer renderer) {
        try {
            actions.removeRelationship(relationshipId);
            return Response.noContent().build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response getRelationships(long nodeId, RelationshipDirection direction, AmpersandSeparatedList types, Renderer renderer) {
        List<RelationshipRepresentation> relreps;
        try {
            relreps = actions.retrieveRelationships(nodeId, direction, types.toArray(new String[types.size()]));
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }

        String entity = renderer.render(relreps.toArray(new RelationshipRepresentation[relreps.size()]));
        return addHeaders(Response.ok(entity, renderer.getMediaType())).build();
    }

    protected Response setRelationshipProperties(long relationshipId, String json, Renderer renderer) {
        json = dodgeStartingUnicodeMarker(json);
        PropertiesMap properties = null;
        try {
            properties = new PropertiesMap(JsonHelper.jsonToMap(json));
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        }

        try {
            actions.setRelationshipProperties(relationshipId, properties);
            return Response.noContent().build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response setRelationshipProperty(long relationshipId, String key, String json, Renderer renderer) {
        json = dodgeStartingUnicodeMarker(json);
        try {
            actions.setRelationshipProperty(relationshipId, key, JsonHelper.jsonToSingleValue(json));
            return Response.noContent().build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        }
    }

    protected Response removeRelationshipProperties(long relationshipId, Renderer renderer) {
        try {
            actions.removeRelationshipProperties(relationshipId);
            return Response.noContent().build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response removeRelationshipProperty(long relationshipId, String propertyKey, Renderer renderer) {
        try {
            if (actions.removeRelationshipProperty(relationshipId, propertyKey)) {
                return Response.noContent().build();
            } else {
                return Response.status(Status.NOT_FOUND).build();
            }
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response getIndexRoot(Renderer renderer) {
        return addHeaders(Response.ok(renderer.render(new IndexRootRepresentation(uriInfo.getBaseUri())), renderer.getMediaType())).build();
    }

    protected Response addToIndex(String indexName, String key, String value, String json, Renderer renderer) {
        json = dodgeStartingUnicodeMarker(json);
        try {
            String objectUri = JsonHelper.jsonToSingleValue(json).toString();
            IndexType indexType = getIndexType(indexName);
            Representation representation = indexType.add(this, indexName, key, value, getNodeIdFromUri(objectUri), renderer);
            return Response.created(new URI(representation.serialize().toString())).build();
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(json, e, renderer);
        } catch (URISyntaxException e) {
            return Response.serverError().build();
        }
    }

    protected Response getObjectFromIndexUri(String indexName, String key, String value, long id, Renderer renderer) {
        IndexType indexType = getIndexType(indexName);
        if (indexType == null) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        try {
            return indexType.get(this, indexName, key, value, id, renderer);
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    protected Response getIndexedObjects(String indexName, String key, String value, Renderer renderer) {
        IndexType indexType = getIndexType(indexName);
        if (indexType == null) {
            return Response.status(Status.BAD_REQUEST).build();
        }
        return indexType.get(this, indexName, key, value, renderer);
    }

    protected Response removeFromIndex(String indexName, String key, String value, long id, Renderer renderer) {
        try {
            IndexType indexType = getIndexType(indexName);
            boolean removed = indexType.remove(this, indexName, key, value, id, renderer);
            return removed ? Response.noContent().build() : Response.status(Status.NOT_FOUND).build();
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    private Response getIndexedNodes(String indexName, String key, String value, Renderer renderer) {
        List<IndexedNodeRepresentation> representations = actions.getIndexedNodes(indexName, key, value);
        String entity = renderer.render(representations.toArray(new IndexedNodeRepresentation[representations.size()]));
        return addHeaders(Response.ok(entity, renderer.getMediaType())).build();
    }

    protected IndexType getIndexType(String indexName) {
        // TODO Node For now :)
        return IndexType.NODE;
    }

    protected enum IndexType {
        NODE {
            @Override
            public Response get(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer)

            {
                if (!service.actions.nodeIsIndexed(indexName, key, value, id)) {
                    return Response.status(Status.NOT_FOUND).build();
                }

                return service.getNode(id, renderer);
            }

            @Override
            public Response get(GenericWebService service, String indexName, String key, String value, Renderer renderer)

            {
                return service.getIndexedNodes(indexName, key, value, renderer);
            }

            @Override
            public Representation add(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer) {
                return service.actions.addNodeToIndex(indexName, key, value, id);
            }

            @Override
            public boolean remove(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer)

            {
                return service.actions.removeNodeFromIndex(indexName, key, value, id);
            }
        },

        RELATIONSHIP {
            @Override
            public Response get(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer)

            {
                return service.getRelationship(id, renderer);
            }

            @Override
            public Response get(GenericWebService service, String indexName, String key, String value, Renderer renderer) {
                throw new UnsupportedOperationException("Not implemented");
            }

            @Override
            public Representation add(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer) {
                throw new UnsupportedOperationException("Not implemented");
            }

            @Override
            public boolean remove(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer) {
                throw new UnsupportedOperationException("Not implemented");
            }
        }

        ;

        abstract Response get(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer);

        abstract Response get(GenericWebService service, String indexName, String key, String value, Renderer renderer);

        abstract Representation add(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer);

        abstract boolean remove(GenericWebService service, String indexName, String key, String value, long id, Renderer renderer);
    }

    protected Response traverse(long startNode, TraverserReturnType returnType, String description, Renderer renderer) {
        description = dodgeStartingUnicodeMarker(description);
        Map<String, Object> map = null;
        try {
            map = description == null || description.length() == 0 ? new HashMap<String, Object>() : JsonHelper.jsonToMap(description);
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(description, e, renderer);
        }

        List<Representation> representations;
        try {
            representations = actions.traverseAndCollect(startNode, map, returnType);
        } catch (NotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        } catch (EvaluationException e) {
            return buildExceptionResponse(Status.INTERNAL_SERVER_ERROR, "Couldn't parse scripts " + "or javax.script isn't correctly on the classpath", e,
                    renderer);
        }
        return addHeaders(Response.ok(renderer.render(representations.toArray(new Representation[representations.size()])), renderer.getMediaType())).build();
    }

    protected Response path(long startNodeId, String description, Renderer renderer) {
        return findPaths(startNodeId, description, true, renderer);
    }

    protected Response paths(long startNodeId, String description, Renderer renderer) {
        return findPaths(startNodeId, description, false, renderer);
    }

    private Response findPaths(long startNodeId, String description, boolean single, Renderer renderer) {
        description = dodgeStartingUnicodeMarker(description);
        long endNodeId;
        Map<String, Object> payload;
        try {
            payload = JsonHelper.jsonToMap(description);
            endNodeId = getNodeIdFromUri((String) payload.get("to"));
        } catch (PropertyValueException e) {
            return buildBadJsonExceptionResponse(description, e, renderer);
        }

        List<PathRepresentation> paths;
        try {
            paths = actions.findPaths(startNodeId, endNodeId, single, payload);
        } catch (StartNodeNotFoundException e) {
            return Response.status(Status.NOT_FOUND).build();
        } catch (EndNodeNotFoundException e) {
            return Response.status(Status.BAD_REQUEST).build();
        } catch (StartNodeSameAsEndNodeException e) {
            return Response.status(Status.BAD_REQUEST).build();
        }

        if (paths.isEmpty()) {
            return Response.status(single ? Status.NOT_FOUND : Status.NO_CONTENT).build();
        }
        return addHeaders(Response.ok(renderer.render(paths.toArray(new PathRepresentation[paths.size()])), renderer.getMediaType())).build();
    }
}
