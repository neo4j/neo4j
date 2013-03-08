/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.awaitIndexState;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.TargetDirectory;

public class SchemaIndexAcceptanceTest
{
    @Test
    public void creatingIndexOnExistingDataBuildsIndexWhichWillBeOnlineNextStartup() throws Exception
    {
        Transaction tx = db.beginTx();
        Node node1 = createNode( label, "name", "One" );
        Node node2 = createNode( label, "name", "Two" );
        Node node3 = createNode( label, "name", "Three" );
        tx.success();
        tx.finish();
        
        tx = db.beginTx();
        IndexDefinition index = db.schema().indexCreator( label ).on( key ).create();
        tx.success();
        tx.finish();
        awaitIndexState( db, index, IndexState.ONLINE );
        
        restart();
        
        assertEquals( IndexState.ONLINE, db.schema().getIndexState( index ) );
    }
    
    private final String storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true ).getAbsolutePath();
    private GraphDatabaseService db;
    private final Label label = label( "label" );
    private final String key = "key";
    
    @Before
    public void before() throws Exception
    {
        db = new EmbeddedGraphDatabase( storeDir );
    }
    
    private void restart()
    {
        db.shutdown();
        db = new EmbeddedGraphDatabase( storeDir );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private Node createNode( Label label, Object... properties )
    {
        Node node = db.createNode( label );
        for ( Map.Entry<String, Object> property : map( properties ).entrySet() )
            node.setProperty( property.getKey(), property.getValue() );
        return node;
    }
}
