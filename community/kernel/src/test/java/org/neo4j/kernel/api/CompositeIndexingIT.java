/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;

@RunWith( Parameterized.class )
public class CompositeIndexingIT
{
    public static final int LABEL_ID = 1;

    @ClassRule
    public static ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public Timeout globalTimeout = Timeout.seconds( 200 );

    private final IndexDescriptor index;
    private GraphDatabaseAPI graphDatabaseAPI;

    @Before
    public void setup() throws Exception
    {
        graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            try ( Statement statement = statement() )
            {
                if ( index.type() == UNIQUE )
                {
                    statement.schemaWriteOperations().uniquePropertyConstraintCreate( index.schema() );
                }
                else
                {
                    statement.schemaWriteOperations().indexCreate( index.schema() );
                }
            }
            tx.success();
        }

        try ( Transaction ignore = graphDatabaseAPI.beginTx();
                Statement statement = statement() )
        {
            while ( statement.readOperations().indexGetState( index ) != InternalIndexState.ONLINE )
            {
                Thread.sleep( 10 );
            } // Will break loop on test timeout
        }
    }

    @After
    public void clean() throws Exception
    {
        try ( Transaction tx = graphDatabaseAPI.beginTx();
                Statement statement = statement() )
        {
            if ( index.type() == UNIQUE )
            {
                statement.schemaWriteOperations().constraintDrop(
                        ConstraintDescriptorFactory.uniqueForSchema( index.schema() ) );
            }
            else
            {
                statement.schemaWriteOperations().indexDrop( index );
            }
            tx.success();
        }

        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            for ( Node node : graphDatabaseAPI.getAllNodes() )
            {
                node.delete();
            }
            tx.success();
        }
    }

    @Parameterized.Parameters( name = "Index: {0}" )
    public static Iterable<Object[]> parameterValues() throws IOException
    {
        return Arrays.asList( Iterators.array( IndexDescriptorFactory.forLabel( LABEL_ID, 1 ) ),
                Iterators.array( IndexDescriptorFactory.forLabel( LABEL_ID, 1, 2 ) ),
                Iterators.array( IndexDescriptorFactory.forLabel( LABEL_ID, 1, 2, 3, 4 ) ),
                Iterators.array( IndexDescriptorFactory.forLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ),
                Iterators.array( IndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 ) ),
                Iterators.array( IndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1, 2 ) ),
                Iterators.array( IndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) )
        );
    }

    public CompositeIndexingIT( IndexDescriptor nodeDescriptor )
    {
        this.index = nodeDescriptor;
    }

    @Test
    public void shouldSeeNodeAddedByPropertyToIndexInTranslation() throws Exception
    {
        try ( Transaction ignore = graphDatabaseAPI.beginTx();
                Statement statement = statement() )
        {
            DataWriteOperations writeOperations = statement.dataWriteOperations();
            long nodeID = writeOperations.nodeCreate();
            writeOperations.nodeAddLabel( nodeID, LABEL_ID );
            for ( int propID : index.schema().getPropertyIds() )
            {
                writeOperations.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            PrimitiveLongIterator resultIterator = seek( statement );
            assertThat( resultIterator.next(), equalTo( nodeID ) );
            assertFalse( resultIterator.hasNext() );
        }
    }

    @Test
    public void shouldSeeNodeAddedToByLabelIndexInTransaction() throws Exception
    {
        try ( Transaction ignore = graphDatabaseAPI.beginTx();
                Statement statement = statement() )
        {
            DataWriteOperations writeOperations = statement.dataWriteOperations();
            long nodeID = writeOperations.nodeCreate();
            for ( int propID : index.schema().getPropertyIds() )
            {
                writeOperations.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            writeOperations.nodeAddLabel( nodeID, LABEL_ID );
            PrimitiveLongIterator resultIterator = seek( statement );
            assertThat( resultIterator.next(), equalTo( nodeID ) );
            assertFalse( resultIterator.hasNext() );
        }
    }

    @Test
    public void shouldNotSeeNodeThatWasDeletedInTransaction() throws Exception
    {
        long nodeID = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx();
              Statement statement = statement() )
        {
            statement.dataWriteOperations().nodeDelete( nodeID );
            assertFalse( seek( statement ).hasNext() );
        }
    }

    @Test
    public void shouldNotSeeNodeThatHasItsLabelRemovedInTransaction() throws Exception
    {
        long nodeID = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx();
                Statement statement = statement() )
        {
            statement.dataWriteOperations().nodeRemoveLabel( nodeID, LABEL_ID );
            assertFalse( seek( statement ).hasNext() );
        }
    }

    @Test
    public void shouldNotSeeNodeThatHasAPropertyRemovedInTransaction() throws Exception
    {
        long nodeID = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx();
              Statement statement = statement() )
        {
            statement.dataWriteOperations().nodeRemoveProperty( nodeID, index.schema().getPropertyIds()[0] );
            assertFalse( seek( statement ).hasNext() );
        }
    }

    @Test
    public void shouldSeeAllNodesAddedInTransaction() throws Exception
    {
        if ( index.type() != UNIQUE ) // this test does not make any sense for UNIQUE indexes
        {
            try ( Transaction ignore = graphDatabaseAPI.beginTx();
                    Statement statement = statement() )
            {
                long nodeID1 = createNode();
                long nodeID2 = createNode();
                long nodeID3 = createNode();
                PrimitiveLongIterator resultIterator = seek( statement );
                Set<Long> result = PrimitiveLongCollections.toSet( resultIterator );
                assertThat( result, contains( nodeID1, nodeID2, nodeID3 ) );
            }
        }
    }

    @Test
    public void shouldSeeAllNodesAddedBeforeTransaction() throws Exception
    {
        if ( index.type() != UNIQUE ) // this test does not make any sense for UNIQUE indexes
        {
            long nodeID1 = createNode();
            long nodeID2 = createNode();
            long nodeID3 = createNode();
            try ( Transaction ignore = graphDatabaseAPI.beginTx();
                  Statement statement = statement() )
            {
                PrimitiveLongIterator resultIterator = seek( statement );
                Set<Long> result = PrimitiveLongCollections.toSet( resultIterator );
                assertThat( result, contains( nodeID1, nodeID2, nodeID3 ) );
            }
        }
    }

    @Test
    public void shouldNotSeeNodesLackingOneProperty() throws Exception
    {
        long nodeID1 = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx();
                Statement statement = statement() )
        {
            DataWriteOperations writeOperations = statement.dataWriteOperations();
            long irrelevantNodeID = writeOperations.nodeCreate();
            writeOperations.nodeAddLabel( irrelevantNodeID, LABEL_ID );
            for ( int i = 0; i < index.schema().getPropertyIds().length - 1; i++ )
            {
                int propID = index.schema().getPropertyIds()[i];
                writeOperations.nodeSetProperty( irrelevantNodeID, propID, Values.intValue( propID ) );
            }
            PrimitiveLongIterator resultIterator = seek( statement );
            Set<Long> result = PrimitiveLongCollections.toSet( resultIterator );
            assertThat( result, contains( nodeID1 ) );
        }
    }

    private long createNode()
            throws InvalidTransactionTypeKernelException, EntityNotFoundException, ConstraintValidationException,
            AutoIndexingKernelException
    {
        long nodeID;
        try ( Transaction tx = graphDatabaseAPI.beginTx() ;
              Statement statement = statement() )
        {
            DataWriteOperations writeOperations = statement.dataWriteOperations();
            nodeID = writeOperations.nodeCreate();
            writeOperations.nodeAddLabel( nodeID, LABEL_ID );
            for ( int propID : index.schema().getPropertyIds() )
            {
                writeOperations.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            tx.success();
        }
        return nodeID;
    }

    private PrimitiveLongIterator seek( Statement statement ) throws IndexNotFoundKernelException,
            IndexNotApplicableKernelException
    {
        return statement.readOperations().indexQuery( index, exactQuery() );
    }

    private IndexQuery[] exactQuery()
    {
        IndexQuery[] query = new IndexQuery[index.schema().getPropertyIds().length];
        for ( int i = 0; i < query.length; i++ )
        {
            int propID = index.schema().getPropertyIds()[i];
            query[i] = IndexQuery.exact( propID, Values.of( propID ) );
        }
        return query;
    }

    private Statement statement()
    {
        return graphDatabaseAPI.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).get();
    }
}
