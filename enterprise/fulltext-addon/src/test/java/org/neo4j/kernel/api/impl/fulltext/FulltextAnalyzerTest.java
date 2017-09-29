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
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_NODES;

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
            fulltextFactory.createFulltextIndex( BLOOM_NODES, NODES, singletonList( "prop" ) );
            provider.init();

            long id;
            try ( Transaction tx = db.beginTx() )
            {
                createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                id = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( BLOOM_NODES, NODES ) )
            {
                assertExactQueryFindsNothing( reader, "and" );
                assertExactQueryFindsNothing( reader, "in" );
                assertExactQueryFindsNothing( reader, "the" );
                assertExactQueryFindsIds( reader, "en", false, id );
                assertExactQueryFindsIds( reader, "och", false, id );
                assertExactQueryFindsIds( reader, "ett", false, id );
            }
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        try ( FulltextProvider provider = createProvider(); )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, SWEDISH, provider );
            fulltextFactory.createFulltextIndex( BLOOM_NODES, NODES, singletonList( "prop" ) );
            provider.init();

            long id;
            try ( Transaction tx = db.beginTx() )
            {
                id = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( BLOOM_NODES, NODES ) )
            {
                assertExactQueryFindsIds( reader, "and", false, id );
                assertExactQueryFindsIds( reader, "in", false, id );
                assertExactQueryFindsIds( reader, "the", false, id );
                assertExactQueryFindsNothing( reader, "en" );
                assertExactQueryFindsNothing( reader, "och" );
                assertExactQueryFindsNothing( reader, "ett" );
            }
        }
    }

    @Test
    public void shouldReindexNodesWhenAnalyzerIsChanged() throws Exception
    {
        long firstID;
        long secondID;
        try ( FulltextProvider provider = createProvider() )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, ENGLISH, provider );
            fulltextFactory.createFulltextIndex( BLOOM_NODES, NODES, singletonList( "prop" ) );
            provider.init();

            try ( Transaction tx = db.beginTx() )
            {
                firstID = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
                secondID = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( BLOOM_NODES, NODES ) )
            {

                assertExactQueryFindsNothing( reader, "and" );
                assertExactQueryFindsNothing( reader, "in" );
                assertExactQueryFindsNothing( reader, "the" );
                assertExactQueryFindsIds( reader, "en", false, secondID );
                assertExactQueryFindsIds( reader, "och", false, secondID );
                assertExactQueryFindsIds( reader, "ett", false, secondID );
            }
        }

        try ( FulltextProvider provider = createProvider() )
        {
            FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, SWEDISH, provider );
            fulltextFactory.createFulltextIndex( BLOOM_NODES, NODES, singletonList( "prop" ) );
            provider.init();
            provider.awaitPopulation();

            try ( ReadOnlyFulltext reader = provider.getReader( BLOOM_NODES, NODES ) )
            {
                assertExactQueryFindsIds( reader, "and",  false, firstID );
                assertExactQueryFindsIds( reader, "in",  false, firstID );
                assertExactQueryFindsIds( reader, "the",  false, firstID );
                assertExactQueryFindsNothing( reader, "en" );
                assertExactQueryFindsNothing( reader, "och" );
                assertExactQueryFindsNothing( reader, "ett" );
            }
        }
    }
}
