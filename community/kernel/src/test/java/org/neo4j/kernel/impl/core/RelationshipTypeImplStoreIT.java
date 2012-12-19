/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

public class RelationshipTypeImplStoreIT
{

    public static final String STORE_DIR = "target/rel-type-test.db";
    public static final int MAX_TYPE_ID = (1 << 16) -1;

    @Before
    public void setUp() throws Exception
    {
        FileUtils.deleteRecursively( new File( STORE_DIR ) );
    }

    @Test
    public void testCreateAndReadAllRelationshipTypes() throws Exception
    {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( STORE_DIR );
        assertEquals( 0, IteratorUtil.count( db.getRelationshipTypes() ) );
        createRelsWithTypes( db, MAX_TYPE_ID );
        checkExceedingIdSpaceFails( db, MAX_TYPE_ID+1);
        checkAllRelationshipTypes( MAX_TYPE_ID, db );
        db.shutdown();
        db = new EmbeddedGraphDatabase( STORE_DIR );
        checkAllRelationshipTypes( MAX_TYPE_ID, db );
        db.shutdown();
    }

    private void checkExceedingIdSpaceFails( EmbeddedGraphDatabase db, int maxTypeId )
    {
        try
        {
            Transaction tx = db.beginTx();
            try
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                createRelationshipAndCheck( db.getNodeManager(), node, node2, maxTypeId );
                fail( "Should not be able to create relationship-type with id " + maxTypeId );
            }
            finally
            {
                tx.success();
                tx.finish();
            }

        }
// TODO      catch ( TransactionFailureException tfe )
        catch ( NullPointerException npe )
        {
// TODO            assertTrue( tfe.getMessage(), tfe.getMessage().contains( "Unable to create relationship type" ) );
        }
    }

    private void checkAllRelationshipTypes( int maxTypeId, EmbeddedGraphDatabase db )
    {
        NodeManager nodeManager = db.getNodeManager();
        for ( int typeId = 0; typeId < maxTypeId; typeId++ )
        {
            Relationship rel = nodeManager.getRelationshipByIdOrNull( (long) typeId );
            RelationshipType relType = nodeManager.getRelationshipTypeById( typeId );
            String idName = "" + typeId;
            assertEquals( idName, relType.name() );
            assertEquals( idName, rel.getType().name() );
        }
    }

    private void createRelsWithTypes( EmbeddedGraphDatabase db, int maxTypeId )
    {
        NodeManager nodeManager = db.getNodeManager();
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            Node node2 = db.createNode();
            for ( int i = 0; i < maxTypeId; i++ )
            {
                createRelationshipAndCheck( nodeManager, node, node2, i );
            }
        }
        finally
        {
            tx.success();
            tx.finish();
        }
    }

    private void createRelationshipAndCheck( NodeManager nodeManager, Node node, Node node2, int typeId )
    {
        DynamicRelationshipType typeName = typeNameForId( typeId );
        Relationship rel = node.createRelationshipTo( node2, typeName );
        assertEquals( typeName.name(),rel.getType().name());
        assertEquals( typeName.name(), nodeManager.getRelationshipTypeById( typeId ).name() );
    }

    private DynamicRelationshipType typeNameForId( int i )
    {
        return DynamicRelationshipType.withName( "" + i );
    }
}
