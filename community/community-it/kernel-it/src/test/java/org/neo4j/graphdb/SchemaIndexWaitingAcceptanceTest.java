/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.index.ControlledPopulationIndexProvider;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
public class SchemaIndexWaitingAcceptanceTest
{
    @Inject
    private GraphDatabaseService database;
    private final ControlledPopulationIndexProvider provider = new ControlledPopulationIndexProvider();

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        List<ExtensionFactory<?>> extensions = Collections.singletonList( singleInstanceIndexProviderFactory( "test", provider ) );
        builder.setExtensions( extensions ).noOpSystemGraphInitializer();
        builder.setConfig( default_schema_provider, provider.getProviderDescriptor().name() );
    }

    @Test
    void shouldTimeoutWaitingForIndexToComeOnline()
    {
        // given
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        IndexDefinition index;
        try ( Transaction tx = database.beginTx() )
        {
            index = tx.schema().indexFor( Label.label( "Person" ) ).on( "name" ).create();
            tx.commit();
        }

        latch.waitForAllToStart();

        var e = assertThrows( IllegalStateException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.schema().awaitIndexOnline( index, 1, TimeUnit.MILLISECONDS );
            }
        } );
        assertThat( e ).hasMessageContaining( "come online" );
        latch.finish();
    }

    @Test
    void shouldTimeoutWaitingForIndexByNameToComeOnline()
    {
        // given
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Person" ) ).on( "name" ).withName( "my_index" ).create();
            tx.commit();
        }

        latch.waitForAllToStart();

        var e = assertThrows( IllegalStateException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.schema().awaitIndexOnline( "my_index", 1, TimeUnit.MILLISECONDS );
            }
        } );
        assertThat( e ).hasMessageContaining( "come online" );
        latch.finish();
    }

    @Test
    void shouldTimeoutWaitingForAllIndexesToComeOnline()
    {
        // given
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Person" ) ).on( "name" ).create();
            tx.commit();
        }

        latch.waitForAllToStart();

        // when
        var e = assertThrows( IllegalStateException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.schema().awaitIndexesOnline( 1, TimeUnit.MILLISECONDS );
            }
        } );
        assertThat( e ).hasMessageContaining( "come online" );
        latch.finish();
    }
}
