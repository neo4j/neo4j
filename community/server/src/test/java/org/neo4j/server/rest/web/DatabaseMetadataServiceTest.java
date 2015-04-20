/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class DatabaseMetadataServiceTest
{
    @Test
    public void shouldAdvertiseRelationshipTypesThatCurrentlyExistInTheDatabase() throws Throwable
    {
        InternalAbstractGraphDatabase db = (InternalAbstractGraphDatabase)new TestGraphDatabaseFactory().newImpermanentDatabase();
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.createRelationshipTo( db.createNode(), withName( "a" ) );
        node.createRelationshipTo( db.createNode(), withName( "b" ) );
        node.createRelationshipTo( db.createNode(), withName( "c" ) );
        tx.success();
        tx.finish();
        
        Database database = new WrappedDatabase( db );
        DatabaseMetadataService service = new DatabaseMetadataService( database );

        tx = db.beginTx();
        Response response = service.getRelationshipTypes();

        assertEquals( 200, response.getStatus() );
        List<Map<String, Object>> jsonList = JsonHelper.jsonToList( response.getEntity()
                .toString() );
        assertEquals( 3, jsonList.size() );
        tx.finish();
        database.stop();
    }
}
