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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class SchemaLoggingIT
{
    private static final String CREATION_FINISHED = "Index creation finished for index [%s].";
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private IndexProviderMap indexProviderMap;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setInternalLogProvider( logProvider );
    }

    @Test
    void shouldLogUserReadableLabelAndPropertyNames()
    {
        String labelName = "User";
        String property = "name";

        // when
        createIndex( db, labelName, property );

        // then
        IndexProvider defaultProvider = indexProviderMap.getDefaultProvider();
        IndexProviderDescriptor providerDescriptor = defaultProvider.getProviderDescriptor();
        String indexName =
                "Index( id=1, name='index_a908f819', type='GENERAL BTREE', schema=(:User {name}), indexProvider='" + providerDescriptor.name() + "' )";

        assertThat( logProvider ).forLevel( INFO )
                .containsMessageWithArguments( "Index population started: [%s]", indexName )
                .containsMessageWithArguments( CREATION_FINISHED, indexName );
    }

    private static void createIndex( GraphDatabaseAPI db, String labelName, String property )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label( labelName ) ).on( property ).create();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }
}
