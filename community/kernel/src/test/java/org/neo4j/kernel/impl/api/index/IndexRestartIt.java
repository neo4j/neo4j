/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class IndexRestartIt
{
    private GraphDatabaseAPI db;
    private final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private TestGraphDatabaseFactory factory;
    private final SchemaIndexProvider mockedIndexProvider = mock(SchemaIndexProvider.class);

    @Test
    public void shouldHandleRestartOfPopulatedIndex() throws Exception
    {
        // Given
        when( mockedIndexProvider.getPopulator( anyLong() )).thenReturn( mock( IndexPopulator.class ) );
        startDb();
        Label myLabel = label( "MyLabel" );

        Transaction tx = db.beginTx();
        db.schema().indexCreator( myLabel ).on( "number_of_bananas_owned" ).create();
        tx.success();
        tx.finish();

        // And Given
        stopDb();
        when( mockedIndexProvider.getInitialState( anyLong() )).thenReturn( InternalIndexState.ONLINE );

        // When
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo(1));

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index), equalTo( Schema.IndexState.ONLINE ) );
        verify( mockedIndexProvider, times( 1 ) ).getPopulator( anyLong() );
        verify( mockedIndexProvider, times( 2 ) ).getWriter( anyLong() );
    }

    @Test
    public void shouldHandleRestartOfPopulatingIndex() throws Exception
    {
        // Given
        when( mockedIndexProvider.getPopulator( anyLong() ) ).thenReturn( mock( IndexPopulator.class ) );
        startDb();
        Label myLabel = label( "MyLabel" );

        Transaction tx = db.beginTx();
        db.schema().indexCreator( myLabel ).on( "number_of_bananas_owned" ).create();
        tx.success();
        tx.finish();

        // And Given
        stopDb();
        when( mockedIndexProvider.getInitialState( anyLong() ) ).thenReturn( InternalIndexState.POPULATING );

        // When
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo( 1 ) );

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index), not( equalTo( Schema.IndexState.FAILED ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong() );
    }

    @Test
    public void shouldHandleRestartWhereIndexWasNotPersisted() throws Exception
    {
        // Given
        when( mockedIndexProvider.getPopulator( anyLong() )).thenReturn( mock( IndexPopulator.class ) );
        startDb();
        Label myLabel = label( "MyLabel" );

        Transaction tx = db.beginTx();
        db.schema().indexCreator( myLabel ).on( "number_of_bananas_owned" ).create();
        tx.success();
        tx.finish();

        // And Given
        stopDb();
        when( mockedIndexProvider.getInitialState( anyLong() )).thenReturn( InternalIndexState.NON_EXISTENT );

        // When
        startDb();

        // Then
        Collection<IndexDefinition> indexes = asCollection( db.schema().getIndexes( myLabel ) );

        assertThat( indexes.size(), equalTo(1));

        IndexDefinition index = single( indexes );
        assertThat( db.schema().getIndexState( index), not( equalTo( Schema.IndexState.FAILED ) ) );
        verify( mockedIndexProvider, times( 2 ) ).getPopulator( anyLong() );
    }

    private void startDb()
    {
        if(db != null)
            db.shutdown();

        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
    }

    private void stopDb()
    {
        if(db != null)
            db.shutdown();
    }

    @Before
    public void before() throws Exception
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( fs );
        factory.setSchemaIndexProviders( Arrays.asList( mockedIndexProvider ) );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
}
