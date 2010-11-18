/**
 * Licensed to Neo Technology under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Neo Technology licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.neo4j.examples.server;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class CreateSimpleGraph {
    
    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    private static final String EMPTY_JSON_CONTAINER = "{}";
    
    public static void main(String[] args) throws URISyntaxException {
        checkDatabaseIsRunning();
        URI firstNode = createNode();
        addProperty(firstNode, "name", "Joe Strummer");
        URI secondNode = createNode();
        addProperty(firstNode, "band", "The Clash");
        URI relationshipUri = addRelationship(firstNode, secondNode, "singer", "{ \"from\" : \"1976\", \"until\" : \"1986\" }");
        addMetadataToProperty(relationshipUri, "stars", "5");
        
        findSingersInBands(firstNode);
    }

    private static void findSingersInBands(URI startNode) throws URISyntaxException {
        // TraversalDescription can be turned into JSON to send to the Server
        TraversalDescription t = new TraversalDescription();
        t.setOrder(TraversalDescription.DEPTH_FIRST);
        t.setUniqueness(TraversalDescription.NODE);
        t.setMaxDepth(10);
        t.setReturnFilter(TraversalDescription.ALL);
        t.setRelationships(new Relationship("singer", Relationship.OUT));
        
        URI traverserUri = new URI(startNode.toString() + "/traverse/node");
        WebResource resource = Client.create().resource(traverserUri); 
        String jsonTraverserPayload = t.toJson();
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(jsonTraverserPayload).post(ClientResponse.class);
        
        System.out.println(String.format("POST [%s] to [%s], status code [%d], returned data: " + System.getProperty("line.separator") + "%s", jsonTraverserPayload, traverserUri, response.getStatus(), response.getEntity(String.class)));
    }

    private static void addMetadataToProperty(URI relationshipUri, String name, String value) throws URISyntaxException {
        URI propertyUri = new URI(relationshipUri.toString() + "/properties");
        WebResource resource = Client.create().resource(propertyUri); 
        
        String entity = toJsonNameValuePairCollection(name, value);
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(entity).put(ClientResponse.class);
        
        System.out.println(String.format("PUT [%s] to [%s], status code [%d]", entity, propertyUri, response.getStatus()));
    }

    private static String toJsonNameValuePairCollection(String name, String value) {
        return String.format("{ \"%s\" : \"%s\" }", name, value);
    }

    private static URI createNode() {
        final String nodeEntryPointUri = SERVER_ROOT_URI + "node";

        WebResource resource = Client.create().resource(nodeEntryPointUri); // http://localhost:7474/db/data/node
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(EMPTY_JSON_CONTAINER).post(ClientResponse.class); // POST {} to the node entry point URI
        
        System.out.println(String.format("POST to [%s], status code [%d], location header [%s]", nodeEntryPointUri, response.getStatus(), response.getLocation().toString()));
        
        return response.getLocation();
    }

    private static URI addRelationship(URI startNode, URI endNode, String relationshipType, String jsonAttributes) throws URISyntaxException {
        URI fromUri = new URI(startNode.toString() + "/relationships");
        String relationshipJson = generateJsonRelationship(endNode, relationshipType, jsonAttributes);
        
        WebResource resource = Client.create().resource(fromUri);
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(relationshipJson).post(ClientResponse.class); // POST JSON to the relationships URI
        
        System.out.println(String.format("POST to [%s], status code [%d], location header [%s]", fromUri, response.getStatus(), response.getLocation().toString()));
        
        return response.getLocation();
    }

    private static String generateJsonRelationship(URI endNode, String relationshipType, String ... jsonAttributes) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"to\" : \"");
        sb.append(endNode.toString());
        sb.append("\", ");
        
        sb.append("\"type\" : \"");
        sb.append(relationshipType);
        if(jsonAttributes == null || jsonAttributes.length < 1) {
            sb.append("\"");            
        } else {
            sb.append("\", \"data\" : ");
            for(int i = 0; i < jsonAttributes.length; i++) {
                sb.append(jsonAttributes[i]);
                if(i < jsonAttributes.length -1) { // Miss off the final comma
                    sb.append(", ");
                }
            }
        }
        
        sb.append(" }");
        return sb.toString();
    }

    private static void addProperty(URI nodeUri, String propertyName, String propertyValue) {
        String propertyUri = nodeUri.toString() + "/properties/" + propertyName;
        
        WebResource resource = Client.create().resource(propertyUri); // http://localhost:7474/db/data/node/{node_id}/properties/name
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON).entity(toJsonStringLiteral(propertyValue)).put(ClientResponse.class);
        
        System.out.println(String.format("PUT to [%s], status code [%d]", propertyUri, response.getStatus()));
    }
    
    private static void checkDatabaseIsRunning() {
        WebResource resource = Client.create().resource(SERVER_ROOT_URI);
        ClientResponse response = resource.get(ClientResponse.class);
        
        System.out.println(String.format("GET on [%s], status code [%d]", SERVER_ROOT_URI, response.getStatus()));
    }
    
    private static String toJsonStringLiteral(String str) {
        return "\"" + str + "\"";
    }
}
