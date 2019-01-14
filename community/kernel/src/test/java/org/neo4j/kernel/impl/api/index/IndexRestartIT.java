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
package org.neo4j.kernel.impl.api.index;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexState;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasSize;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.haveState;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class IndexRestartIT
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private GraphDatabaseService db;
    private TestGraphDatabaseFactory factory;
    private final ControlledPopulationIndexProvider provider = new ControlledPopulationIndexProvider();
    private final Label myLabel = label( "MyLabel" );

    @Before
    public void before()
    {
        factory = new TestGraphDatabaseFactory();
        factory.setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs.get() ) );
        factory.setKernelExtensions( Collections.singletonList(
            singleInstanceIndexProviderFactory( "test", provider )
        ) );
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    /* This is somewhat difficult to test since dropping an index while it's populating forces it to be cancelled
     * first (and also awaiting cancellation to complete). So this is a best-effort to have the timing as close
     * as possible. If this proves to be flaky, remove it right away.
     */
    @Test
    public void shouldBeAbleToDropIndexWhileItIsPopulating()
    {
        // GIVEN
        startDb();
        DoubleLatch populationCompletionLatch = provider.installPopulationJobCompletionLatch();
        IndexDefinition index = createIndex();
        populationCompletionLatch.waitForAllToStart(); // await population job to start

        // WHEN
        dropIndex( index, populationCompletionLatch );

        // THEN
        assertThat( getIndexes( db, myLabel ), inTx( db, hasSize( 0 ) ) );
        try
        {
            getIndexState( db, index );
            fail( "This index should have been deleted" );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), CoreMatchers.containsString( myLabel.name() ) );
        }
    }

    @Test
    public void shouldHandleRestartOfOnlineIndex()
    {
        // Given
        startDb();
        createIndex();
        provider.awaitFullyPopulated();

        // And Given
        stopDb();
        provider.setInitialIndexState( ONLINE );

        // When
        startDb();

        // Then
        assertThat( getIndexes( db, myLabel ), inTx( db, haveState( db, Schema.IndexState.ONLINE ) ) );
        assertEquals( 1, provider.populatorCallCount.get() );
        assertEquals( 2, provider.writerCallCount.get() );
    }

    @Test
    public void shouldHandleRestartIndexThatHasNotComeOnlineYet()
    {
        // Given
        startDb();
        createIndex();

        // And Given
        stopDb();
        provider.setInitialIndexState( POPULATING );

        // When
        startDb();

        assertThat( getIndexes( db, myLabel ), inTx( db, not( haveState( db, Schema.IndexState.FAILED ) ) ) );
        assertEquals( 2, provider.populatorCallCount.get() );
    }

    private IndexDefinition createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( myLabel ).on( "number_of_bananas_owned" ).create();
            tx.success();
            return index;
        }
    }

    private void dropIndex( IndexDefinition index, DoubleLatch populationCompletionLatch )
    {
        try ( Transaction tx = db.beginTx() )
        {
            index.drop();
            populationCompletionLatch.finish();
            tx.success();
        }
    }

    private void startDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }

        db = factory.newImpermanentDatabase();
    }

    private void stopDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }
}
