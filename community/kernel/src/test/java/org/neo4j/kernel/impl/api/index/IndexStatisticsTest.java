/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.register.Register.DoubleLongRegister;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

@Ignore( "FIXME: reenable/review when refactor of index counts is completed" )
public class IndexStatisticsTest
{
    @Test
    public void shouldProvideIndexStatisticsForDataCreatedBeforeTheIndexOnceIndexIsOnline() throws KernelException
    {
        // given
        createSomePersons();

        // when
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // then
        assertEquals( 4l, indexSize( index ) );
        assertEquals( 0.75d, indexSelectivity( index ), TOLERANCE );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedAfterTheIndexOnceIndexIsOnline() throws KernelException
    {
        // given
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // when
        createSomePersons();

        // then
        assertEquals( 4l, indexSize( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedBeforeAndAfterTheIndexOnceIndexIsOnline()
            throws KernelException
    {
        // given
        createSomePersons();

        // when
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );
        createSomePersons();

        // then
        assertEquals( 8l, indexSize( index ) );
        // selectivity after population without the newly created nodes
        assertEquals( 0.75d, indexSelectivity( index ), TOLERANCE );
    }

    @Test
    public void shouldRemoveIndexStatisticsAfterIndexIsDeleted() throws KernelException
    {
        // given
        createSomePersons();
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // when
        dropIndex( index );

        // then
        try
        {
            indexSize( index );
            fail( "Expected IndexNotFoundKernelException to be thrown" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            assertEquals( 0l, getTracker().indexSize( index.getLabelId(), index.getPropertyKeyId() ) );
        }

        // and also
        try
        {
            indexSelectivity( index );
            fail( "Expected IndexNotFoundKernelException to be thrown" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            DoubleLongRegister register = Registers.newDoubleLongRegister();
            getTracker().indexSample( index.getLabelId(), index.getPropertyKeyId(), register );
            assertDoubleLongEquals( 0l, 0l, register );
        }
    }

    @Test
    public void shouldProvideIndexSelectivityWhenThereAreManyDuplicates() throws KernelException
    {
        // given some initial data
        int created = 0;
        while ( created < 10000 )
        {
            created += createNamedPeople();
        }

        // when
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // then
        assertCorrectIndexSize( created, indexSize( index ) );
        double expectedSelectivity = UNIQUE_NAMES / ( (double) created);
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditions() throws KernelException
    {
        // given some initial data
        int createdBefore = 0;
        while ( createdBefore < 10000 )
        {
            createdBefore += createNamedPeople();
        }

        // when populating while creating
        IndexDescriptor index = createIndex( "Person", "name" );
        int createdAfter = 0;
        int updatesSeenWhilePopulating = -1;
        while ( createdAfter < 10000 )
        {
            createdAfter += createNamedPeople();
            if ( createdAfter % 5 == 0)
            {
                if ( updatesSeenWhilePopulating < 0 && isIndexOnline( index ) )
                {
                    updatesSeenWhilePopulating = createdAfter;
                }
            }
        }
        awaitOnline( index );

        if ( updatesSeenWhilePopulating == -1 )
        {
            updatesSeenWhilePopulating = createdAfter;
        }

        // then
        int expected = createdBefore + createdAfter;
        assertCorrectIndexSize( expected, indexSize( index ) );

        // selectivity after population without sampling
        int seenWhilePopulating = createdBefore + updatesSeenWhilePopulating;
        double expectedSelectivity = UNIQUE_NAMES / ( (double) seenWhilePopulating );
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndDeletions()
            throws KernelException
    {
        // given some initial data
        ArrayList<Long> nodes = new ArrayList<>( 10000 );
        int createdBefore = 0;
        while ( createdBefore < 10000 )
        {
            createdBefore += createNamedPeople( nodes );
        }

        // when populating while creating
        IndexDescriptor index = createIndex( "Person", "name" );
        int createdAfter = 0;
        int updatesSeenWhilePopulating = -1;
        while ( createdAfter < 10000 )
        {
            createdAfter += createNamedPeople( nodes );
            if ( createdAfter % 5 == 0 )
            {
                long nodeId = nodes.get( nodes.size() / 2 );
                deleteNode( nodeId );
                createdAfter--;

                if ( updatesSeenWhilePopulating < 0 && isIndexOnline( index ) )
                {
                    updatesSeenWhilePopulating = createdAfter;
                }
            }
        }
        awaitOnline( index );

        if ( updatesSeenWhilePopulating == -1 )
        {
            updatesSeenWhilePopulating = createdAfter;
        }

        // then
        int expected = createdBefore + createdAfter;
        assertCorrectIndexSize( expected, indexSize( index ) );

        // selectivity after population without sampling
        int seenWhilePopulating = createdBefore + updatesSeenWhilePopulating;
        double expectedSelectivity = UNIQUE_NAMES / ( (double) seenWhilePopulating );
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndChanges()
            throws KernelException
    {
        // given some initial data
        ArrayList<Long> nodes = new ArrayList<>( 10000 );
        int createdBefore = 0;
        while ( createdBefore < 10000 )
        {
            createdBefore += createNamedPeople( nodes );
        }

        // when populating while creating
        IndexDescriptor index = createIndex( "Person", "name" );
        int createdAfter = 0;
        int updatesSeenWhilePopulating = -1;
        while ( createdAfter < 10000 )
        {
            createdAfter += createNamedPeople( nodes );
            if ( createdAfter % 5 == 0 )
            {
                long nodeId = nodes.get( nodes.size() / 2 );
                changeName( nodeId, "name", NAMES[ (int) (nodeId % NAMES.length) ] );

                if ( updatesSeenWhilePopulating < 0 && isIndexOnline( index ) )
                {
                    updatesSeenWhilePopulating = createdAfter;
                }
            }
        }
        awaitOnline( index );

        if ( updatesSeenWhilePopulating == -1 )
        {
            updatesSeenWhilePopulating = createdAfter;
        }

        // then
        int expected = createdBefore + createdAfter;
        assertCorrectIndexSize( expected, indexSize( index ) );

        // selectivity after population without sampling
        int seenWhilePopulating = createdBefore + updatesSeenWhilePopulating;
        double expectedSelectivity = UNIQUE_NAMES / ( (double) seenWhilePopulating );
        assertCorrectIndexSelectivity( expectedSelectivity, indexSelectivity( index ) );
    }

    private void deleteNode( long nodeId ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            statement.dataWriteOperations().nodeDelete( nodeId );
            tx.success();
        }
    }

    private void changeName( long nodeId, String propertyKeyName, Object newValue ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKeyName );
            statement.dataWriteOperations().nodeSetProperty( nodeId, Property.property( propertyKeyId, newValue ) );
            tx.success();
        }
    }

    private int createNamedPeople() throws KernelException
    {
        return createNamedPeople( null );
    }

    private int createNamedPeople( Collection<Long> nodeSet ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            for ( String name : NAMES )
            {
                long nodeId = createNode( statement, "Person", "name", name );
                if ( nodeSet != null )
                {
                    nodeSet.add( nodeId );
                }
            }
            tx.success();
        }
        return NAMES.length;
    }

    private void dropIndex( IndexDescriptor index ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            statement.schemaWriteOperations().indexDrop( index );
            tx.success();
        }
    }

    private boolean isIndexOnline( IndexDescriptor indexDescriptor ) throws IndexNotFoundKernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            InternalIndexState state = statement.readOperations().indexGetState( indexDescriptor );
            tx.success();
            return state == InternalIndexState.ONLINE;
        }
    }

    private long indexSize( IndexDescriptor descriptor ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            long indexSize = statement.readOperations().indexSize( descriptor );
            tx.success();
            return indexSize;
        }
    }

    private double indexSelectivity( IndexDescriptor descriptor ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            double selectivity = statement.readOperations().indexUniqueValuesSelectivity( descriptor );
            tx.success();
            return selectivity;
        }
    }

    private CountsTracker getTracker()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( NeoStore.class ).getCounts();
    }

    private void createSomePersons() throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            createNode( statement, "Person", "name", "Davide" );
            createNode( statement, "Person", "name", "Stefan" );
            createNode( statement, "Person", "name", "John" );
            createNode( statement, "Person", "name", "John" );
            tx.success();
        }
    }

    private long createNode( Statement statement, String labelName, String propertyKeyName,
                             Object value ) throws KernelException
    {
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( labelName );
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKeyName );
        long nodeId = statement.dataWriteOperations().nodeCreate();
        statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
        statement.dataWriteOperations().nodeSetProperty( nodeId, Property.property( propertyKeyId, value ) );
        return nodeId;
    }

    private IndexDescriptor createIndex( String labelName, String propertyKeyName ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( labelName );
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKeyName );
            IndexDescriptor index = statement.schemaWriteOperations().indexCreate( labelId, propertyKeyId );
            tx.success();
            return index;
        }
    }

    private IndexDescriptor awaitOnline( IndexDescriptor index ) throws KernelException
    {
        long start = System.currentTimeMillis();
        long end = start + 3000;
        while ( System.currentTimeMillis() < end )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Statement statement = bridge.instance();
                switch ( statement.readOperations().indexGetState( index ) )
                {
                    case ONLINE:
                        return index;

                    case FAILED:
                        throw new IllegalStateException( "Index failed instead of becoming ONLINE" );

                    default:
                        break;
                }
                tx.success();

                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    // ignored
                }
            }
        }
        throw new IllegalStateException( "Index did not become ONLINE within reasonable time" );
    }

    private void assertDoubleLongEquals( long expectedUniqueValue, long expectedSampledSize,
                                         DoubleLongRegister register )
    {
        assertEquals( expectedUniqueValue, register.readFirst() );
        assertEquals( expectedSampledSize, register.readSecond() );
    }

    private static void assertCorrectIndexSize( long expected, long actual )
    {
        String message = String.format(
            "Expected number of entries to not differ by more than 10 (expected: %d actual: %d)", expected, actual
        );
        // TODO: fix this assertion to be precise
        assertTrue( message, Math.abs( expected - actual ) < 100 );
    }

    private static void assertCorrectIndexSelectivity( double expected, double actual )
    {
        String message = String.format(
            "Expected number of entries to not differ by more than 10 (expected: %f actual: %f with tollerance: %f)",
            expected, actual, TOLERANCE
        );
        assertEquals( message, expected, actual, TOLERANCE );
    }

    private static final double UNIQUE_NAMES = 10.0;
    private static final String[] NAMES = new String[] {
        "Andres", "Davide", "Jakub", "Chris", "Tobias", "Stefan", "Petra", "Rickard", "Mattias", "Emil", "Chris", "Chris"
    };

    private static final double TOLERANCE = 0.00001d;

    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule();
    private GraphDatabaseService db;
    private ThreadToStatementContextBridge bridge;

    @Before
    public void before()
    {
        GraphDatabaseAPI graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        this.db = graphDatabaseAPI;
        DependencyResolver dependencyResolver = graphDatabaseAPI.getDependencyResolver();
        this.bridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
    }
}
