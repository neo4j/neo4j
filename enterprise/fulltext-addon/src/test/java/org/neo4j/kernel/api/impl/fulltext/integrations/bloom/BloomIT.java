/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BloomIT
{
    public static final String NODES = "CALL db.fulltext.bloomFulltextNodes([\"%s\"])";
    public static final String RELS = "CALL db.fulltext.bloomFulltextRelationships([\"%s\"])";
    public static final String ENTITYID = "entityid";
    @Rule
    public final FileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private TestGraphDatabaseFactory factory;
    private GraphDatabaseService db;

    @Before
    public void before() throws Exception
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs.get() );
        factory.addKernelExtensions( Collections.singletonList( new BloomKernelExtensionFactory() ) );
        db = factory.newImpermanentDatabase( testDirectory.graphDbDir(),
                Collections.singletonMap( LoadableBloomFulltextConfig.bloom_indexed_properties, "prop, relprop" ) );
    }

    @Test
    public void shouldPopulateAndQueryIndexes() throws Exception
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( "relprop", "They relate" );
            transaction.success();
        }

        Result result = db.execute( String.format( NODES, "integration" ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertEquals( 1L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
        result = db.execute( String.format( RELS, "relate" ) );
        assertEquals( 0L, result.next().get( ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
}
