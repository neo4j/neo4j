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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static java.util.Collections.singletonList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.NODES;

public class FulltextAnalyzerTest extends LuceneFulltextTestSupport
{
    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, new EnglishAnalyzer() );
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello and hello again, in the end." );
                node2.setProperty( "prop", "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "bloomNodes", NODES ) )
            {
                assertExactQueryFindsNothing( reader, "and" );
                assertExactQueryFindsNothing( reader, "in" );
                assertExactQueryFindsNothing( reader, "the" );
                assertExactQueryFindsIds( reader, "en", secondID );
                assertExactQueryFindsIds( reader, "och", secondID );
                assertExactQueryFindsIds( reader, "ett", secondID );
            }
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        FulltextFactory fulltextFactory = new FulltextFactory( fs, storeDir, new SwedishAnalyzer() );
        try ( FulltextProvider provider = createProvider(); )
        {
            fulltextFactory.createFulltextIndex( "bloomNodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello and hello again, in the end." );
                node2.setProperty( "prop", "En apa och en tomte bodde i ett hus." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "bloomNodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "and", firstID );
                assertExactQueryFindsIds( reader, "in", firstID );
                assertExactQueryFindsIds( reader, "the", firstID );
                assertExactQueryFindsNothing( reader, "en" );
                assertExactQueryFindsNothing( reader, "och" );
                assertExactQueryFindsNothing( reader, "ett" );
            }
        }
    }
}
