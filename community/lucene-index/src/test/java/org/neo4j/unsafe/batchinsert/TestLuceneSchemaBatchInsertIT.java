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
package org.neo4j.unsafe.batchinsert;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.map;

public class TestLuceneSchemaBatchInsertIT
{
    @Test
    public void shouldLoadAndUseLuceneProvider() throws Exception
    {
        // GIVEN
        File storeDir = testDirectory.graphDbDir();
        BatchInserter inserter = BatchInserters.inserter( storeDir );
        inserter.createDeferredSchemaIndex( LABEL ).on( "name" ).create();

        // WHEN
        inserter.createNode( map( "name", "Mattias" ), LABEL );
        inserter.shutdown();

        // THEN
        GraphDatabaseFactory graphDatabaseFactory = new TestGraphDatabaseFactory();
        GraphDatabaseAPI db = (GraphDatabaseAPI) graphDatabaseFactory.newEmbeddedDatabase( storeDir );
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        SchemaIndexProvider schemaIndexProvider = dependencyResolver.resolveDependency(
                SchemaIndexProvider.class,
                SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );

        // assert the indexProvider is a Lucene one
        try ( Transaction ignore = db.beginTx() )
        {
            IndexDefinition indexDefinition = single( db.schema().getIndexes( LABEL ) );
            assertThat( db.schema().getIndexState( indexDefinition ), is( Schema.IndexState.ONLINE ) );
            assertThat( schemaIndexProvider, instanceOf( LuceneSchemaIndexProvider.class ) );
        }

        // CLEANUP
        db.shutdown();
    }

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private static final Label LABEL = label( "Person" );
}
