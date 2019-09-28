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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;

@DbmsExtension
class ConstraintCreationIT
{
    @Inject
    private GraphDatabaseAPI db;

    private static final Label LABEL = Label.label( "label1" );
    private static final long indexId = 1;

    @ExtensionCallback
    void configureLuceneSubProvider( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( default_schema_provider, NATIVE30.providerName() );
    }

    @Test
    @DbmsExtension( configurationCallback = "configureLuceneSubProvider" )
    void shouldNotLeaveLuceneIndexFilesHangingAroundIfConstraintCreationFails()
    {
        // given
        attemptAndFailConstraintCreation();

        // then
        IndexProvider indexProvider = db.getDependencyResolver().resolveDependency( IndexProviderMap.class ).getDefaultProvider();
        File indexDir = indexProvider.directoryStructure().directoryForIndex( indexId );

        assertFalse( indexDir.exists() );
    }

    @Test
    void shouldNotLeaveNativeIndexFilesHangingAroundIfConstraintCreationFails()
    {
        // given
        attemptAndFailConstraintCreation();

        // then
        IndexProvider indexProvider =
                db.getDependencyResolver().resolveDependency( IndexProviderMap.class ).getDefaultProvider();
        File indexDir = indexProvider.directoryStructure().directoryForIndex( indexId );

        assertFalse( indexDir.exists() );
    }

    private void attemptAndFailConstraintCreation()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 2; i++ )
            {
                Node node1 = tx.createNode( LABEL );
                node1.setProperty( "prop", true );
            }

            tx.commit();
        }

        // when
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( "prop" ).create();
            fail( "Should have failed with ConstraintViolationException" );
            tx.commit();
        }
        catch ( ConstraintViolationException ignored )
        {
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 0, Iterables.count( tx.schema().getIndexes() ) );
        }
    }
}
