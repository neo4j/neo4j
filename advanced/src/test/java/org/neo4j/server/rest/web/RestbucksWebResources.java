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

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.NeoServer;
import org.neo4j.server.database.Database;

@Path("/restbucks")
public class RestbucksWebResources {

    private @Context
    UriInfo uriInfo;
    private Database database;

    public RestbucksWebResources() {
        database = NeoServer.server().database();
    }
    
    public RestbucksWebResources(Database database) {
        this.database = database;
    }

    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/order")
    @POST
    public Response createOrder(String coffee) throws URISyntaxException {

        GraphDatabaseService graphDb = NeoServer.server().database().db;

        Transaction tx = graphDb.beginTx();
        long orderNo = -1;
        try {
            Node myNode = graphDb.createNode();
            myNode.setProperty("lineItem", coffee);
            orderNo = myNode.getId();

            tx.success();
        } finally {
            tx.finish();
        }

        return Response.created(new URI(uriInfo.getRequestUri().toString() + "/" + orderNo)).entity("{ order_number : " + orderNo + "}").build();
    }

    @Produces(MediaType.APPLICATION_JSON)
    @Path("/order/{orderId}")
    @GET
    public Response getOrder(@PathParam("orderId") long orderId) {
        
        Transaction tx = database.db.beginTx();
        try {
            Node node = database.db.getNodeById(orderId);
            tx.success();
            return Response.ok(node.getProperty("lineItem")).build();
        } finally {
            tx.finish();
        }
    }

}
