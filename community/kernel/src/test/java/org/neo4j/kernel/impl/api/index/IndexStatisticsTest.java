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

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
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
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        assertEquals( 4l, numberOfIndexEntries( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedAfterTheIndexOnceIndexIsOnline() throws KernelException
    {
        // given
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );

        // when
        createSomePersons();

        // then
        assertEquals( 4l, numberOfIndexEntries( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsForDataCreatedBeforeAndAfterTheIndexOnceIndexIsOnline() throws
            KernelException
    {
        // given
        createSomePersons();

        // when
        IndexDescriptor index = awaitOnline( createIndex( "Person", "name" ) );
        createSomePersons();

        // then
        assertEquals( 8l, numberOfIndexEntries( index ) );
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
            numberOfIndexEntries( index );
            fail( "Expected IndexNotFoundKernelException to be thrown" );
        }
        catch ( IndexNotFoundKernelException e )
        {
            // expected
        }
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
        while ( createdAfter < 10000 )
        {
            createdAfter += createNamedPeople();
        }
        awaitOnline( index );

        // then
        int expected = createdBefore + createdAfter;
        assertEquals( expected, numberOfIndexEntries( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndDeletions() throws
            KernelException
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
        while ( createdAfter < 10000 )
        {
            createdAfter += createNamedPeople( nodes );
            if ( createdAfter % 5 == 0 )
            {
                long nodeId = nodes.get( nodes.size() / 2 );
                deleteNode( nodeId );
                createdAfter--;
            }
        }
        awaitOnline( index );

        // then
        int expected = createdBefore + createdAfter;
        assertEquals( expected, numberOfIndexEntries( index ) );
    }

    @Test
    public void shouldProvideIndexStatisticsWhenIndexIsBuiltViaPopulationAndConcurrentAdditionsAndChanges() throws
            KernelException
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
        while ( createdAfter < 10000 )
        {
            createdAfter += createNamedPeople( nodes );
            if ( createdAfter % 5 == 0 )
            {
                long nodeId = nodes.get( nodes.size() / 2 );
                changeName( nodeId, "name", names[ (int) (nodeId % names.length) ] );
            }
        }
        awaitOnline( index );

        // then
        int expected = createdBefore + createdAfter;
        assertEquals( expected, numberOfIndexEntries( index ) );
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
            for ( String name : names )
            {
                long nodeId = createNode( statement, "Person", "name", name );
                if ( nodeSet != null )
                {
                    nodeSet.add( nodeId );
                }
            }
            tx.success();
        }
        return names.length;
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

    private long numberOfIndexEntries( IndexDescriptor descriptor ) throws KernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = bridge.instance();
            long numberOfEntries = statement.readOperations().indexNumberOfEntries( descriptor );
            tx.success();
            return numberOfEntries;
        }
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

    private String[] names = new String[] {
        "Andres", "Davide", "Jakub", "Chris", "Tobias", "Stefan", "Petra", "Rickard", "Mattias", "Emil", "Chris", "Chris"
    };

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
