/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.ControlledPopulationSchemaIndexProvider;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;

public class SchemaIndexWaitingAcceptanceTest
{
    private final ControlledPopulationSchemaIndexProvider provider = new ControlledPopulationSchemaIndexProvider();

    @Rule
    public ImpermanentDatabaseRule rule = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory databaseFactory )
        {
            List<KernelExtensionFactory<?>> extensions;
            extensions = Collections.<KernelExtensionFactory<?>>singletonList( singleInstanceSchemaIndexProviderFactory(
                    "test", provider ) );
            databaseFactory.addKernelExtensions( extensions );
        }
    };

    @Test
    public void shouldTimeoutWatingForIndexToComeOnline() throws Exception
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseService();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( DynamicLabel.label( "Person" ) ).on( "name" ).create();
            tx.success();
        }

        latch.awaitStart();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // then
            db.schema().awaitIndexOnline( index, 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage(), containsString( "come online" ) );
        }
        finally
        {
            latch.finish();
        }
    }

    @Test
    public void shouldTimeoutWatingForAllIndexesToComeOnline() throws Exception
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseService();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( DynamicLabel.label( "Person" ) ).on( "name" ).create();
            tx.success();
        }

        latch.awaitStart();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // then
            db.schema().awaitIndexesOnline( 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage(), containsString( "come online" ) );
        }
        finally
        {
            latch.finish();
        }
    }
}
