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

import java.util.Optional;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

import static org.neo4j.storageengine.api.EntityType.NODE;

public class FulltextAnalyzerTest extends LuceneFulltextTestSupport
{
    public static final String ENGLISH = "english";
    public static final String SWEDISH = "swedish";

    @Test
    public void shouldBeAbleToSpecifyEnglishAnalyzer() throws Exception
    {
        applySetting( FulltextConfig.fulltext_default_analyzer, ENGLISH );

        SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[]{LABEL.name()}, settings, PROP );
        IndexReference nodes;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            nodes = schemaWrite.indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
            transaction.success();
        }
        await( nodes );

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            createNodeIndexableByPropertyValue( LABEL, "Hello and hello again, in the end." );
            id = createNodeIndexableByPropertyValue( LABEL, "En apa och en tomte bodde i ett hus." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, "nodes", "and" );
            assertQueryFindsNothing( ktx, "nodes", "in" );
            assertQueryFindsNothing( ktx, "nodes", "the" );
            assertQueryFindsIds( ktx, "nodes", "en", id );
            assertQueryFindsIds( ktx, "nodes", "och", id );
            assertQueryFindsIds( ktx, "nodes", "ett", id );
        }
    }

    @Test
    public void shouldBeAbleToSpecifySwedishAnalyzer() throws Exception
    {
        applySetting( FulltextConfig.fulltext_default_analyzer, SWEDISH );
        SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[]{LABEL.name()}, settings, PROP );
        IndexReference nodes;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            nodes = schemaWrite.indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
            transaction.success();
        }
        await( nodes );

        long id;
        try ( Transaction tx = db.beginTx() )
        {
            id = createNodeIndexableByPropertyValue( LABEL, "Hello and hello again, in the end." );
            createNodeIndexableByPropertyValue( LABEL, "En apa och en tomte bodde i ett hus." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, "nodes", "and", id );
            assertQueryFindsIds( ktx, "nodes", "in", id );
            assertQueryFindsIds( ktx, "nodes", "the", id );
            assertQueryFindsNothing( ktx, "nodes", "en" );
            assertQueryFindsNothing( ktx, "nodes", "och" );
            assertQueryFindsNothing( ktx, "nodes", "ett" );
        }
    }

    @Test
    public void shouldNotReindexNodesWhenDefaultAnalyzerIsChanged() throws Exception
    {
        long firstID;
        long secondID;
        applySetting( FulltextConfig.fulltext_default_analyzer, ENGLISH );
        SchemaDescriptor descriptor = fulltextAdapter.schemaFor( NODE, new String[]{LABEL.name()}, settings, PROP );
        IndexReference nodes;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            nodes = schemaWrite.indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
            transaction.success();
        }
        await( nodes );

        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Hello and hello again, in the end." );
            secondID = createNodeIndexableByPropertyValue( LABEL, "En apa och en tomte bodde i ett hus." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, "nodes", "and" );
            assertQueryFindsNothing( ktx, "nodes", "in" );
            assertQueryFindsNothing( ktx, "nodes", "the" );
            assertQueryFindsIds( ktx, "nodes", "en", secondID );
            assertQueryFindsIds( ktx, "nodes", "och", secondID );
            assertQueryFindsIds( ktx, "nodes", "ett", secondID );
        }

        applySetting( FulltextConfig.fulltext_default_analyzer, SWEDISH );
        try ( KernelTransactionImplementation ktx = getKernelTransaction() )
        {
            SchemaRead schemaRead = ktx.schemaRead();
            await( schemaRead.indexGetForName( "nodes" ) );
            // These results should be exactly the same as before the configuration change and restart.
            assertQueryFindsNothing( ktx, "nodes", "and" );
            assertQueryFindsNothing( ktx, "nodes", "in" );
            assertQueryFindsNothing( ktx, "nodes", "the" );
            assertQueryFindsIds( ktx, "nodes", "en", secondID );
            assertQueryFindsIds( ktx, "nodes", "och", secondID );
            assertQueryFindsIds( ktx, "nodes", "ett", secondID );
        }
    }
}
