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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;

public class FulltextAnalyzerTest extends LuceneFulltextTestSupport
{
    private static final String ENGLISH = "english";
    static final String SWEDISH = "swedish";
    private static final String FOLDING = "standard-folding";

    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        applySetting( FulltextSettings.fulltext_default_analyzer, ENGLISH );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( FULLTEXT ).withName( "nodes" ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "nodes", 30, TimeUnit.SECONDS );
        }

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            createNodeIndexableByPropertyValue( tx, LABEL, "Hello and hello again, in the end." );
            id = createNodeIndexableByPropertyValue( tx, LABEL, "En apa och en tomte bodde i ett hus." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, true, "nodes", "and" );
            assertQueryFindsNothing( ktx, true, "nodes", "in" );
            assertQueryFindsNothing( ktx, true, "nodes", "the" );
            assertQueryFindsIds( ktx, true, "nodes", "en", id );
            assertQueryFindsIds( ktx, true, "nodes", "och", id );
            assertQueryFindsIds( ktx, true, "nodes", "ett", id );
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        applySetting( FulltextSettings.fulltext_default_analyzer, SWEDISH );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( FULLTEXT ).withName( "nodes" ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "nodes", 30, TimeUnit.SECONDS );
        }

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            id = createNodeIndexableByPropertyValue( tx, LABEL, "Hello and hello again, in the end." );
            createNodeIndexableByPropertyValue( tx, LABEL, "En apa och en tomte bodde i ett hus." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, "nodes", "and", id );
            assertQueryFindsIds( ktx, true, "nodes", "in", id );
            assertQueryFindsIds( ktx, true, "nodes", "the", id );
            assertQueryFindsNothing( ktx, true, "nodes", "en" );
            assertQueryFindsNothing( ktx, true, "nodes", "och" );
            assertQueryFindsNothing( ktx, true, "nodes", "ett" );
        }
    }

    @Test
    public void shouldBeAbleToSpecifyFoldingAnalyzer() throws Exception
    {
        applySetting( FulltextSettings.fulltext_default_analyzer, FOLDING );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( FULLTEXT ).withName( "nodes" ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "nodes", 30, TimeUnit.SECONDS );
        }

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            id = createNodeIndexableByPropertyValue( tx, LABEL, "Příliš žluťoučký kůň úpěl ďábelské ódy." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, "nodes", "prilis", id );
            assertQueryFindsIds( ktx, true, "nodes", "zlutoucky", id );
            assertQueryFindsIds( ktx, true, "nodes", "kun", id );
            assertQueryFindsIds( ktx, true, "nodes", "upel", id );
            assertQueryFindsIds( ktx, true, "nodes", "dabelske", id );
            assertQueryFindsIds( ktx, true, "nodes", "ody", id );
        }
    }

    @Test
    public void shouldNotReindexNodesWhenDefaultAnalyzerIsChanged() throws Exception
    {
        long secondID;
        applySetting( FulltextSettings.fulltext_default_analyzer, ENGLISH );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( FULLTEXT ).withName( "nodes" ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "nodes", 30, TimeUnit.SECONDS );
        }

        try ( Transaction tx = db.beginTx() )
        {
            createNodeIndexableByPropertyValue( tx, LABEL, "Hello and hello again, in the end." );
            secondID = createNodeIndexableByPropertyValue( tx, LABEL, "En apa och en tomte bodde i ett hus." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, true, "nodes", "and" );
            assertQueryFindsNothing( ktx, true, "nodes", "in" );
            assertQueryFindsNothing( ktx, true, "nodes", "the" );
            assertQueryFindsIds( ktx, true, "nodes", "en", secondID );
            assertQueryFindsIds( ktx, true, "nodes", "och", secondID );
            assertQueryFindsIds( ktx, true, "nodes", "ett", secondID );
        }

        applySetting( FulltextSettings.fulltext_default_analyzer, SWEDISH );
        try ( KernelTransactionImplementation ktx = getKernelTransaction() )
        {
            SchemaRead schemaRead = ktx.schemaRead();
            await( schemaRead.indexGetForName( "nodes" ) );
            // These results should be exactly the same as before the configuration change and restart.
            assertQueryFindsNothing( ktx, true, "nodes", "and" );
            assertQueryFindsNothing( ktx, true, "nodes", "in" );
            assertQueryFindsNothing( ktx, true, "nodes", "the" );
            assertQueryFindsIds( ktx, true, "nodes", "en", secondID );
            assertQueryFindsIds( ktx, true, "nodes", "och", secondID );
            assertQueryFindsIds( ktx, true, "nodes", "ett", secondID );
        }
    }
}
