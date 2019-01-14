/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package schema;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexWriter;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;


public class IndexValuesValidationTest
{
    @ClassRule
    public static final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static GraphDatabaseService database;

    @BeforeClass
    public static void setUp()
    {
        database = new GraphDatabaseFactory().newEmbeddedDatabase( directory.graphDbDir() );
    }

    @AfterClass
    public static void tearDown()
    {
        database.shutdown();
    }

    @Test
    public void validateIndexedNodeProperties()
    {
        Label label = Label.label( "indexedNodePropertiesTestLabel" );
        String propertyName = "indexedNodePropertyName";

        createIndex( label, propertyName );

        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
        }

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( propertyName, StringUtils.repeat( "a", IndexWriter.MAX_TERM_LENGTH + 1 ) );
            transaction.success();
        }
    }

    @Test
    public void validateNodePropertiesOnPopulation()
    {
        Label label = Label.label( "populationTestNodeLabel" );
        String propertyName = "populationTestPropertyName";

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( propertyName, StringUtils.repeat( "a", IndexWriter.MAX_TERM_LENGTH + 1 ) );
            transaction.success();
        }

        IndexDefinition indexDefinition = createIndex( label, propertyName );
        try
        {
            try ( Transaction ignored = database.beginTx() )
            {
                database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
            }
        }
        catch ( IllegalStateException e )
        {
            try ( Transaction ignored = database.beginTx() )
            {
                String indexFailure = database.schema().getIndexFailure( indexDefinition );
                assertThat( "", indexFailure, Matchers.containsString(
                        "java.lang.IllegalArgumentException: Index key-value size it to large. Please see index documentation for limitations." ) );
            }
        }
    }

    @Test
    public void validateExplicitIndexedNodeProperties()
    {
        Label label = Label.label( "explicitIndexedNodePropertiesTestLabel" );
        String propertyName = "explicitIndexedNodeProperties";
        String explicitIndexedNodeIndex = "explicitIndexedNodeIndex";

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            database.index().forNodes( explicitIndexedNodeIndex )
                    .add( node, propertyName, "shortString" );
            transaction.success();
        }

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Property value size is too large for index. Please see index documentation for limitations." );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            String longValue = StringUtils.repeat( "a", IndexWriter.MAX_TERM_LENGTH + 1 );
            database.index().forNodes( explicitIndexedNodeIndex )
                    .add( node, propertyName, longValue );
            transaction.success();
        }
    }

    @Test
    public void validateExplicitIndexedRelationshipProperties()
    {
        Label label = Label.label( "explicitIndexedRelationshipPropertiesTestLabel" );
        String propertyName = "explicitIndexedRelationshipProperties";
        String explicitIndexedRelationshipIndex = "explicitIndexedRelationshipIndex";
        RelationshipType indexType = RelationshipType.withName( "explicitIndexType" );

        try ( Transaction transaction = database.beginTx() )
        {
            Node source = database.createNode( label );
            Node destination = database.createNode( label );
            Relationship relationship = source.createRelationshipTo( destination, indexType );
            database.index().forRelationships( explicitIndexedRelationshipIndex )
                    .add( relationship, propertyName, "shortString" );
            transaction.success();
        }

        expectedException.expect( IllegalArgumentException.class );
        expectedException.expectMessage( "Property value size is too large for index. Please see index documentation for limitations." );
        try ( Transaction transaction = database.beginTx() )
        {
            Node source = database.createNode( label );
            Node destination = database.createNode( label );
            Relationship relationship = source.createRelationshipTo( destination, indexType );
            String longValue = StringUtils.repeat( "a", IndexWriter.MAX_TERM_LENGTH + 1 );
            database.index().forRelationships( explicitIndexedRelationshipIndex )
                    .add( relationship, propertyName, longValue );
            transaction.success();
        }
    }

    private IndexDefinition createIndex( Label label, String propertyName )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            IndexDefinition indexDefinition = database.schema().indexFor( label ).on( propertyName ).create();
            transaction.success();
            return indexDefinition;
        }
    }
}
