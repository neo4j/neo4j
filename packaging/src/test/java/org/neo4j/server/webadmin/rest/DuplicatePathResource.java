///**
// * Copyright (c) 2002-2010 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.neo4j.server.webadmin.rest;
//
//import javax.ws.rs.GET;
//import javax.ws.rs.Path;
//import javax.ws.rs.Produces;
//import javax.ws.rs.core.Context;
//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response;
//import javax.ws.rs.core.UriInfo;
//
//import org.neo4j.server.database.Database;
//
//@Path( "testing/" )
//public class DuplicatePathResource
//{
//
//    public DuplicatePathResource( @Context Database database, @Context UriInfo uriInfo )
//    {
//    }
//
//
//    @GET
//    @Produces( MediaType.APPLICATION_JSON )
//    public Response getOne(  )
//    {
//        return Response.ok( "getOne" ).build();
//    }
//    @GET
//    @Produces( MediaType.APPLICATION_JSON )
//    public Response getTwo(  )
//    {
//        return Response.ok( "getTwo" ).build();
//    }
//
//}
