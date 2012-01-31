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
package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Don't extend Neo4jTestCase since these tests restarts the db in the tests. 
 */
public class TestRecovery
{
    private String getDbPath()
    {
        return "target/var/recovery";
    }
    
    private GraphDatabaseService newGraphDbService()
    {
        String path = getDbPath();
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        return new EmbeddedGraphDatabase( path );
    }
    
    @Test
    public void testRecovery() throws Exception
    {
        GraphDatabaseService graphDb = newGraphDbService();
        Index<Node> nodeIndex = graphDb.index().forNodes( "node-index" );
        Index<Relationship> relIndex = graphDb.index().forRelationships( "rel-index" );
        RelationshipType relType = DynamicRelationshipType.withName( "recovery" );
        
        graphDb.beginTx();
        Node node = graphDb.createNode();
        Node otherNode = graphDb.createNode();
        Relationship rel = node.createRelationshipTo( otherNode, relType );
        nodeIndex.add( node, "key1", "string value" ); 
        nodeIndex.add( node, "key2", 12345 ); 
        relIndex.add( rel, "key1", "string value" ); 
        relIndex.add( rel, "key2", 12345 ); 
        graphDb.shutdown();
        
        // Start up and let it recover
        final GraphDatabaseService newGraphDb = new EmbeddedGraphDatabase( getDbPath() );
        newGraphDb.shutdown();
    }
    
    @Test
    public void testAsLittleAsPossibleRecoveryScenario() throws Exception
    {
        GraphDatabaseService db = newGraphDbService();
        Index<Node> index = db.index().forNodes( "my-index" );
        db.beginTx();
        Node node = db.createNode();
        index.add( node, "key", "value" );
        db.shutdown();
        
        // This doesn't seem to trigger recovery... it really should
        new EmbeddedGraphDatabase( getDbPath() ).shutdown();
    }
    
    @Test
    public void testIndexDeleteIssue() throws Exception
    {
        GraphDatabaseService db = newGraphDbService();
        db.index().forNodes( "index" );
        db.shutdown();
        
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                AddDeleteQuit.class.getName(), getDbPath() } ).waitFor() );
        
        new EmbeddedGraphDatabase( getDbPath() ).shutdown();
        db.shutdown();
    }

    @Test
    public void recoveryForRelationshipCommandsOnly() throws Exception
    {
        String path = getDbPath();
        Neo4jTestCase.deleteFileOrDirectory( new File( path ) );
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                AddRelToIndex.class.getName(), getDbPath() } ).waitFor() );
        
        // I would like to do this, but there's no exception propagated out from the constructor
        // if the recovery fails.
        // new EmbeddedGraphDatabase( getDbPath() ).shutdown();
        
        // Instead I have to do this
        FileSystemAbstraction fileSystem = CommonFactories.defaultFileSystemAbstraction();
        Map<Object, Object> params = MapUtil.genericMap(
                "store_dir", getDbPath(),
                IndexStore.class, new IndexStore( getDbPath(), fileSystem ),
                FileSystemAbstraction.class, fileSystem,
                StringLogger.class, StringLogger.DEV_NULL,
                LogBufferFactory.class, CommonFactories.defaultLogBufferFactory() );
        LuceneDataSource ds = new LuceneDataSource( params );
        ds.close();
    }
}
