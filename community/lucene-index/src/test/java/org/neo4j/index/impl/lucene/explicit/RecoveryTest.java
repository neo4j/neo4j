/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.RelationshipType.withName;

/**
 * Don't extend Neo4jTestCase since these tests restarts the db in the tests.
 */
public class RecoveryTest
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void testRecovery() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Node otherNode = db.createNode();
            Relationship rel = node.createRelationshipTo( otherNode, withName( "recovery" ) );
            db.index().forNodes( "node-index" ).add( node, "key1", "string value" );
            db.index().forNodes( "node-index" ).add( node, "key2", 12345 );
            db.index().forRelationships( "rel-index" ).add( rel, "key1", "string value" );
            db.index().forRelationships( "rel-index" ).add( rel, "key2", 12345 );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void shouldNotAcceptValuesWithNullToString() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Node otherNode = db.createNode();
            Relationship rel = node.createRelationshipTo( otherNode, DynamicRelationshipType.withName( "recovery" ) );
            Index<Node> nodeIndex = db.index().forNodes( "node-index" );
            RelationshipIndex relationshipIndex = db.index().forRelationships( "rel-index" );

            // Add
            assertAddFailsWithIllegalArgument( nodeIndex, node, "key1", new ClassWithToStringAlwaysNull() );
            assertAddFailsWithIllegalArgument( relationshipIndex, rel, "key1", new ClassWithToStringAlwaysNull() );

            // Remove
            assertRemoveFailsWithIllegalArgument( nodeIndex, node, "key1", new ClassWithToStringAlwaysNull() );
            assertRemoveFailsWithIllegalArgument( relationshipIndex, rel, "key1", new ClassWithToStringAlwaysNull() );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void testAsLittleAsPossibleRecoveryScenario() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "my-index" ).add( db.createNode(), "key", "value" );
            tx.success();
        }

        forceRecover();
    }

    @Test
    public void testIndexDeleteIssue() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "index" );
            tx.success();
        }
        shutdownDB();

        db.ensureStarted();
        Index<Node> index;
        Index<Node> index2;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( "index" );
            index2 = db.index().forNodes( "index2" );
            Node node = db.createNode();
            index.add( node, "key", "value" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            index.delete();
            index2.add( db.createNode(), "key", "value" );
            tx.success();
        }
        db.shutdown();

        db.ensureStarted();
        forceRecover();
    }

    @Test
    public void recoveryForRelationshipCommandsOnly() throws Throwable
    {
        // shutdown db here
        File storeDir = db.getStoreDirFile();
        shutdownDB();

        try ( Transaction tx = db.beginTx() )
        {
            Index<Relationship> index = db.index().forRelationships( "myIndex" );
            Node node = db.createNode();
            Relationship relationship = db.createNode().createRelationshipTo( node,
                    RelationshipType.withName( "KNOWS" ) );

            index.add( relationship, "key", "value" );
            tx.success();
        }

        db.shutdown();

        Config config = Config.defaults();
        IndexConfigStore indexStore = new IndexConfigStore( storeDir, fileSystemRule.get() );
        LuceneDataSource ds = new LuceneDataSource( storeDir, config, indexStore, fileSystemRule.get(), OperationalMode.single );
        ds.start();
        ds.stop();
    }

    @Test
    public void recoveryOnDeletedIndex() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( "index" );
            tx.success();
        }

        // shutdown db here
        shutdownDB();

        Index<Node> index;
        Index<Node> index2;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( "index" );
            index2 = db.index().forNodes( "index2" );
            Node node = db.createNode();
            index.add( node, "key", "value" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            index.delete();
            index2.add( db.createNode(), "key", "value" );
            tx.success();
        }

        db.shutdownAndKeepStore();

        db.ensureStarted();

        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( db.index().existsForNodes( "index" ) );
            assertNotNull( db.index().forNodes( "index2" ).get( "key", "value" ).getSingle() );
        }
    }

    private void shutdownDB()
    {
        db.shutdownAndKeepStore();
    }

    private void forceRecover() throws IOException
    {
        db.restartDatabase();
    }

    static class ClassWithToStringAlwaysNull
    {
        @Override
        public String toString()
        {
            return null;
        }

    }

    private <ENTITY extends PropertyContainer> void assertAddFailsWithIllegalArgument( Index<ENTITY> index,
            ENTITY entity, String key, Object value )
    {
        try
        {
            index.add( entity, key, value );
            fail( "Should not accept value with null toString" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }

    private <ENTITY extends PropertyContainer> void assertRemoveFailsWithIllegalArgument( Index<ENTITY> index,
            ENTITY entity, String key, Object value )
    {
        try
        {
            index.remove( entity, key, value );
            fail( "Should not accept value with null toString" );
        }
        catch ( IllegalArgumentException e )
        {
            // Good
        }
    }
}
