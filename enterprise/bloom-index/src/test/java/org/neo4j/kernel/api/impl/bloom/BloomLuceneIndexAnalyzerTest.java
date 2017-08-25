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
package org.neo4j.kernel.api.impl.bloom;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class BloomLuceneIndexAnalyzerTest
{
    @ClassRule
    public static FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();

    private static final Label LABEL = Label.label( "label" );
    private static final RelationshipType RELTYPE = RelationshipType.withName( "type" );

    @Test
    public void shouldUseTokenizingAnalyzerAsDefault() throws Exception
    {
        GraphDatabaseAPI db = dbRule.withSetting( GraphDatabaseSettings.bloom_indexed_properties, "prop" ).getGraphDatabaseAPI();
        Config config = db.getDependencyResolver().resolveDependency( Config.class );
        config.augment( GraphDatabaseSettings.bloom_indexed_properties, "prop" );
        try ( BloomIndex bloomIndex = new BloomIndex( fileSystemRule, testDirectory.graphDbDir(), config ) )
        {
            db.registerTransactionEventHandler( bloomIndex.getUpdater() );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "prop",  "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( BloomIndexReader reader = bloomIndex.getNodeReader() )
            {

                assertEquals( firstID, reader.query( "hello" ).next() );
                assertEquals( secondID, reader.query( "en" ).next() );
                assertEquals( secondID, reader.query( "och" ).next() );
                assertEquals( secondID, reader.query( "ett" ).next() );
            }
        }
    }
    @Test
    public void shouldBeAbleToSpecifyKeyWordAnalyzer() throws Exception
    {
        DatabaseRule rule = dbRule;
        rule.withSetting( GraphDatabaseSettings.bloom_indexed_properties, "prop" );
        rule.withSetting( GraphDatabaseSettings.bloom_analyzer, "org.apache.lucene.analysis.sv.SwedishAnalyzer" );
        GraphDatabaseAPI db = rule.getGraphDatabaseAPI();
        Config config = db.getDependencyResolver().resolveDependency( Config.class );
        config.augment( GraphDatabaseSettings.bloom_indexed_properties, "prop" );
        config.augment( GraphDatabaseSettings.bloom_analyzer, "org.apache.lucene.analysis.sv.SwedishAnalyzer" );
        try ( BloomIndex bloomIndex = new BloomIndex( fileSystemRule, testDirectory.graphDbDir(), config ) )
        {
            db.registerTransactionEventHandler( bloomIndex.getUpdater() );

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                Node node2 = db.createNode( LABEL );
                secondID = node2.getId();
                node2.setProperty( "prop", "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( BloomIndexReader reader = bloomIndex.getNodeReader() )
            {
                assertEquals( firstID, reader.query( "hello" ).next() );
                assertFalse( reader.query( "en" ).hasNext() );
                assertFalse( reader.query( "och" ).hasNext() );
                assertFalse( reader.query( "ett" ).hasNext() );
            }
        }
    }
}
