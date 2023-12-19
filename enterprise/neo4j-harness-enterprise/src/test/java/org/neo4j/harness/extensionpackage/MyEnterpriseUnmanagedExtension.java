/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.harness.extensionpackage;

import org.eclipse.jetty.http.HttpStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

@Path( "myExtension" )
public class MyEnterpriseUnmanagedExtension
{
    private final GraphDatabaseService db;

    public MyEnterpriseUnmanagedExtension( @Context GraphDatabaseService db )
    {
        this.db = db;
    }

    @GET
    @Path( "doSomething" )
    public Response doSomething()
    {
        return Response.status( 234 ).build();
    }

    @GET
    @Path( "createConstraint" )
    public Response createProperty()
    {
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = db.execute( "CREATE CONSTRAINT ON (user:User) ASSERT exists(user.name)" ) )
            {
                // nothing to-do
            }
            tx.success();
            return Response.status( HttpStatus.CREATED_201 ).build();
        }
        catch ( Exception e )
        {
            return Response.status( HttpStatus.NOT_IMPLEMENTED_501 ).build();
        }
    }
}
