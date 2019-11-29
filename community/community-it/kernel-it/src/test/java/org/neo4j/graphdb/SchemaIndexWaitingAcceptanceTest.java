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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.index.ControlledPopulationIndexProvider;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;

public class SchemaIndexWaitingAcceptanceTest
{
    private final ControlledPopulationIndexProvider provider = new ControlledPopulationIndexProvider();

    @Rule
    public final DbmsRule rule = new ImpermanentDbmsRule()
    {
        @Override
        protected void configure( DatabaseManagementServiceBuilder databaseFactory )
        {
            super.configure( databaseFactory );
            List<ExtensionFactory<?>> extensions = Collections.singletonList( singleInstanceIndexProviderFactory( "test", provider ) );
            ((TestDatabaseManagementServiceBuilder) databaseFactory).setExtensions( extensions ).noOpSystemGraphInitializer();
        }
    }.withSetting( default_schema_provider, provider.getProviderDescriptor().name() );

    @Test
    public void shouldTimeoutWaitingForIndexToComeOnline()
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseAPI();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = tx.schema().indexFor( Label.label( "Person" ) ).on( "name" ).create();
            tx.commit();
        }

        latch.waitForAllToStart();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // then
            tx.schema().awaitIndexOnline( index, 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage() ).contains( "come online" );
        }
        finally
        {
            latch.finish();
        }
    }

    @Test
    public void shouldTimeoutWaitingForIndexByNameToComeOnline()
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseAPI();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Person" ) ).on( "name" ).withName( "my_index" ).create();
            tx.commit();
        }

        latch.waitForAllToStart();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // then
            tx.schema().awaitIndexOnline( "my_index", 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage() ).contains( "come online" );
        }
        finally
        {
            latch.finish();
        }
    }

    @Test
    public void shouldTimeoutWaitingForAllIndexesToComeOnline()
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseAPI();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Person" ) ).on( "name" ).create();
            tx.commit();
        }

        latch.waitForAllToStart();

        // when
        try ( Transaction tx = db.beginTx() )
        {
            // then
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage() ).contains( "come online" );
        }
        finally
        {
            latch.finish();
        }
    }
}
