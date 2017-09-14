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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.time.Clock;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.NODES;

public class FulltextAnalyzerTest
{
    private static final Label LABEL = Label.label( "label" );
    private static final NullLog LOG = NullLog.getInstance();
    @ClassRule
    public static FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();

    private AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clock.systemDefaultZone(), LOG );

    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        File storeDir = testDirectory.graphDbDir();
        FulltextFactory fulltextFactory = new FulltextFactory( fileSystemRule, storeDir, new EnglishAnalyzer() );

        try ( FulltextProvider provider = new FulltextProvider( db, LOG, availabilityGuard ) )
        {
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                Node node2 = db.createNode( LABEL );
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello and hello again, in the end." );
                node2.setProperty( "prop", "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "bloomNodes", NODES ) )
            {

                assertFalse( reader.query( "and" ).hasNext() );
                assertFalse( reader.query( "in" ).hasNext() );
                assertFalse( reader.query( "the" ).hasNext() );
                assertEquals( secondID, reader.query( "en" ).next() );
                assertEquals( secondID, reader.query( "och" ).next() );
                assertEquals( secondID, reader.query( "ett" ).next() );
            }
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        File storeDir = testDirectory.graphDbDir();
        FulltextFactory fulltextFactory = new FulltextFactory( fileSystemRule, storeDir, new SwedishAnalyzer() );

        try ( FulltextProvider provider = new FulltextProvider( db, LOG, availabilityGuard ); )
        {
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                Node node2 = db.createNode( LABEL );
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello and hello again, in the end." );
                node2.setProperty( "prop", "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "bloomNodes", NODES ) )
            {
                assertEquals( firstID, reader.query( "and" ).next() );
                assertEquals( firstID, reader.query( "in" ).next() );
                assertEquals( firstID, reader.query( "the" ).next() );
                assertFalse( reader.query( "en" ).hasNext() );
                assertFalse( reader.query( "och" ).hasNext() );
                assertFalse( reader.query( "ett" ).hasNext() );
            }
        }
    }
}
