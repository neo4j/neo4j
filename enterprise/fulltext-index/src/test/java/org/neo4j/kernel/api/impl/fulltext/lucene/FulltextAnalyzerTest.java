/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.kernel.api.impl.fulltext.FulltextConfig;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

import static org.neo4j.storageengine.api.EntityType.NODE;

public class FulltextAnalyzerTest extends LuceneFulltextTestSupport
{
    private static final String ENGLISH = EnglishAnalyzer.class.getCanonicalName();
    private static final String SWEDISH = SwedishAnalyzer.class.getCanonicalName();

    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        applySetting( FulltextConfig.fulltext_default_analyzer, ENGLISH );

        MultiTokenSchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], PROP );
        IndexReference nodes;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            nodes = schemaWrite.indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( "nodes" ) );
            transaction.success();
        }
        await( nodes );

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
            id = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsNothing( "nodes", "and" );
            assertQueryFindsNothing( "nodes", "in" );
            assertQueryFindsNothing( "nodes", "the" );
            assertQueryFindsIds( "nodes", "en", id );
            assertQueryFindsIds( "nodes", "och", id );
            assertQueryFindsIds( "nodes", "ett", id );
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        applySetting( FulltextConfig.fulltext_default_analyzer, SWEDISH );
        MultiTokenSchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], PROP );
        IndexReference nodes;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            nodes = schemaWrite.indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( "nodes" ) );
            transaction.success();
        }
        await( nodes );

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            id = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
            createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( "nodes", "and", id );
            assertQueryFindsIds( "nodes", "in", id );
            assertQueryFindsIds( "nodes", "the", id );
            assertQueryFindsNothing( "nodes", "en" );
            assertQueryFindsNothing( "nodes", "och" );
            assertQueryFindsNothing( "nodes", "ett" );
        }
    }

    @Test
    public void shouldReindexNodesWhenAnalyzerIsChanged() throws Exception
    {
        long firstID;
        long secondID;
        applySetting( FulltextConfig.fulltext_default_analyzer, ENGLISH );
        MultiTokenSchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[0], PROP );
        IndexReference nodes;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            nodes = schemaWrite.indexCreate( descriptor, Optional.of( FulltextIndexProviderFactory.DESCRIPTOR.name() ),
                    Optional.of( "nodes" ) );
            transaction.success();
        }
        await( nodes );

        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello and hello again, in the end." );
            secondID = createNodeIndexableByPropertyValue( "En apa och en tomte bodde i ett hus." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {

            assertQueryFindsNothing( "nodes", "and" );
            assertQueryFindsNothing( "nodes", "in" );
            assertQueryFindsNothing( "nodes", "the" );
            assertQueryFindsIds( "nodes", "en", secondID );
            assertQueryFindsIds( "nodes", "och", secondID );
            assertQueryFindsIds( "nodes", "ett", secondID );
        }

        applySetting( FulltextConfig.fulltext_default_analyzer, SWEDISH );
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaRead schemaRead = transaction.schemaRead();
            await( schemaRead.indexGetForName( "nodes" ) );
            assertQueryFindsIds( "nodes", "and", firstID );
            assertQueryFindsIds( "nodes", "in", firstID );
            assertQueryFindsIds( "nodes", "the", firstID );
            assertQueryFindsNothing( "nodes", "en" );
            assertQueryFindsNothing( "nodes", "och" );
            assertQueryFindsNothing( "nodes", "ett" );
        }
    }
}
