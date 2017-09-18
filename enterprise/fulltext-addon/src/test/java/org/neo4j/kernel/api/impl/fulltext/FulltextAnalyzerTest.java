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
import org.junit.Test;

import org.neo4j.graphdb.Transaction;

import static java.util.Collections.singletonList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.NODES;

public class FulltextAnalyzerTest extends LuceneFulltextTestSupport
{
    private static final String ENGLISH = EnglishAnalyzer.class.getCanonicalName();
    private static final String SWEDISH = SwedishAnalyzer.class.getCanonicalName();

    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, ENGLISH, provider );
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ) );
            provider.init();

            long id;
            try ( Transaction tx = db.beginTx() )
            {
                createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                id = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "bloomNodes", NODES ) )
            {
                assertExactQueryFindsNothing( reader, "and" );
                assertExactQueryFindsNothing( reader, "in" );
                assertExactQueryFindsNothing( reader, "the" );
                assertExactQueryFindsIds( reader, "en", id );
                assertExactQueryFindsIds( reader, "och", id );
                assertExactQueryFindsIds( reader, "ett", id );
            }
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        try ( FulltextProvider provider = createProvider(); )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, SWEDISH, provider );
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ) );
            provider.init();

            long id;
            try ( Transaction tx = db.beginTx() )
            {
                id = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "bloomNodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "and", id );
                assertExactQueryFindsIds( reader, "in", id );
                assertExactQueryFindsIds( reader, "the", id );
                assertExactQueryFindsNothing( reader, "en" );
                assertExactQueryFindsNothing( reader, "och" );
                assertExactQueryFindsNothing( reader, "ett" );
            }
        }
    }

    @Test
    public void shouldReindexNodesWhenAnalyzerIsChanged() throws Exception
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        File storeDir = testDirectory.graphDbDir();

        long firstID;
        long secondID;
        try ( FulltextProvider provider = new FulltextProvider( db, LOG, availabilityGuard ) )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fileSystemRule, storeDir, ENGLISH, provider );
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ) );
            provider.init();

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

        try ( FulltextProvider provider = new FulltextProvider( db, LOG, availabilityGuard ); )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fileSystemRule, storeDir, SWEDISH, provider );
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ) );
            provider.init();
            provider.awaitPopulation();

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
