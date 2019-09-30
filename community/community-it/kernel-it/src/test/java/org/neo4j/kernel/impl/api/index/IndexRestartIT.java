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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceIndexProviderFactory;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexState;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasSize;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.haveState;

@EphemeralTestDirectoryExtension
class IndexRestartIT
{
    @Inject
    private FileSystemAbstraction fs;

    private GraphDatabaseService db;
    private TestDatabaseManagementServiceBuilder factory;
    private final ControlledPopulationIndexProvider provider = new ControlledPopulationIndexProvider();
    private final Label myLabel = label( "MyLabel" );
    private DatabaseManagementService managementService;

    @BeforeEach
    void before()
    {
        factory = new TestDatabaseManagementServiceBuilder();
        factory.setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) );
        factory.setExtensions( List.of( singleInstanceIndexProviderFactory( "test", provider ) ) );
    }

    @AfterEach
    void after()
    {
        managementService.shutdown();
    }

    /* This is somewhat difficult to test since dropping an index while it's populating forces it to be cancelled
     * first (and also awaiting cancellation to complete). So this is a best-effort to have the timing as close
     * as possible. If this proves to be flaky, remove it right away.
     */
    @Test
    void shouldBeAbleToDropIndexWhileItIsPopulating()
    {
        // GIVEN
        startDb();
        DoubleLatch populationCompletionLatch = provider.installPopulationJobCompletionLatch();
        IndexDefinition index = createIndex();
        populationCompletionLatch.waitForAllToStart(); // await population job to start

        // WHEN
        dropIndex( index, populationCompletionLatch );
        try ( Transaction transaction = db.beginTx() )
        {
            // THEN
            assertThat( getIndexes( transaction, myLabel ), hasSize( 0 ) );
            var e = assertThrows( NotFoundException.class, () -> getIndexState( transaction, index ) );
            assertThat( e.getMessage(), CoreMatchers.containsString( myLabel.name() ) );
        }
    }

    @Test
    void shouldHandleRestartOfOnlineIndex()
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
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( transaction, myLabel ), haveState( transaction, Schema.IndexState.ONLINE ) );
        }
        assertEquals( 1, provider.populatorCallCount.get() );
        assertEquals( 2, provider.writerCallCount.get() );
    }

    @Test
    void shouldHandleRestartIndexThatHasNotComeOnlineYet()
    {
        // Given
        startDb();
        createIndex();

        // And Given
        stopDb();
        provider.setInitialIndexState( POPULATING );

        // When
        startDb();

        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( transaction, myLabel ), not( haveState( transaction, Schema.IndexState.FAILED ) ) );
        }
        assertEquals( 2, provider.populatorCallCount.get() );
    }

    private IndexDefinition createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( myLabel ).on( "number_of_bananas_owned" ).create();
            tx.commit();
            return index;
        }
    }

    private void dropIndex( IndexDefinition index, DoubleLatch populationCompletionLatch )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getIndexByName( index.getName() ).drop();
            populationCompletionLatch.finish();
            tx.commit();
        }
    }

    private void startDb()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }

        managementService = factory
                .impermanent()
                .noOpSystemGraphInitializer()
                .setConfig( default_schema_provider, provider.getProviderDescriptor().name() )
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void stopDb()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }
}
