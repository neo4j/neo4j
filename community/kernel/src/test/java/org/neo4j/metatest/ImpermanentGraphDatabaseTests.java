/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.metatest;

import static junit.framework.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentGraphDatabase;

public class ImpermanentGraphDatabaseTests
{
    private ImpermanentGraphDatabase db;

    @Before
	public void Given() {
		db = new ImpermanentGraphDatabase( "target/var/ineodb" );
	}
    
    @After
    public void tearDown()
    {
        db.shutdown();
    }

	@Test
    public void should_keep_data_between_start_and_shutdown()
    {
        createNode();
        
        assertEquals( "Expected one new node, plus reference node", 2, nodeCount() );
    }
	
	@Test
    public void should_keep_reference_node()
    {
        createNode();
        assertEquals( "Expected one new node, plus reference node", 2, nodeCount() );
        db.cleanContent( true );
        assertEquals( "reference node", 1, nodeCount() );
        db.cleanContent( false );
        assertEquals( "reference node", 0, nodeCount() );
    }

    @Test
    public void data_should_not_survive_shutdown()
    {
        createNode();
        db.shutdown();

        db = new ImpermanentGraphDatabase( "neodb");
        
        assertEquals( "Should not see anything but the default reference node.", 1, nodeCount() );
    }
    
	private int nodeCount() {
		return IteratorUtil.count(db.getAllNodes());
	}

	private void createNode() {
		Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
	}
}
