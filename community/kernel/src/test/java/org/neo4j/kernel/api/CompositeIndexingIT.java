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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.UNIQUE;
import static org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference.fromDescriptor;

@RunWith( Parameterized.class )
public class CompositeIndexingIT
{
    private static final int LABEL_ID = 1;

    @ClassRule
    public static ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public Timeout globalTimeout = Timeout.seconds( 200 );

    private final SchemaIndexDescriptor index;
    private GraphDatabaseAPI graphDatabaseAPI;

    @Before
    public void setup() throws Exception
    {
        graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            if ( index.type() == UNIQUE )
            {
                ktx.schemaWrite().uniquePropertyConstraintCreate( index.schema() );
            }
            else
            {
                ktx.schemaWrite().indexCreate( index.schema(), null );
            }
            tx.success();
        }

        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            while ( ktx.schemaRead().indexGetState( fromDescriptor( index ) ) !=
                    InternalIndexState.ONLINE )
            {
                Thread.sleep( 10 );
            } // Will break loop on test timeout
        }
    }

    @After
    public void clean() throws Exception
    {
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            if ( index.type() == UNIQUE )
            {
                ktx.schemaWrite().constraintDrop(
                        ConstraintDescriptorFactory.uniqueForSchema( index.schema() ) );
            }
            else
            {
                ktx.schemaWrite().indexDrop( fromDescriptor( index ) );
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
    public static Iterable<Object[]> parameterValues()
    {
        return Arrays.asList( Iterators.array( SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1 ) ),
                Iterators.array( SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1, 2 ) ),
                Iterators.array( SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1, 2, 3, 4 ) ),
                Iterators.array( SchemaIndexDescriptorFactory.forLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ),
                Iterators.array( SchemaIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1 ) ),
                Iterators.array( SchemaIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1, 2 ) ),
                Iterators.array( SchemaIndexDescriptorFactory.uniqueForLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) )
        );
    }

    public CompositeIndexingIT( SchemaIndexDescriptor nodeDescriptor )
    {
        this.index = nodeDescriptor;
    }

    @Test
    public void shouldSeeNodeAddedByPropertyToIndexInTranslation() throws Exception
    {
        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            Write write = ktx.dataWrite();
            long nodeID = write.nodeCreate();
            write.nodeAddLabel( nodeID, LABEL_ID );
            for ( int propID : index.schema().getPropertyIds() )
            {
                write.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertTrue( cursor.next() );
                assertThat( cursor.nodeReference(), equalTo( nodeID ) );
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldSeeNodeAddedToByLabelIndexInTransaction() throws Exception
    {
        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            Write write = ktx.dataWrite();
            long nodeID = write.nodeCreate();
            for ( int propID : index.schema().getPropertyIds() )
            {
                write.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            write.nodeAddLabel( nodeID, LABEL_ID );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertTrue( cursor.next() );
                assertThat( cursor.nodeReference(), equalTo( nodeID ) );
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldNotSeeNodeThatWasDeletedInTransaction() throws Exception
    {
        long nodeID = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            ktx.dataWrite().nodeDelete( nodeID );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldNotSeeNodeThatHasItsLabelRemovedInTransaction() throws Exception
    {
        long nodeID = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            ktx.dataWrite().nodeRemoveLabel( nodeID, LABEL_ID );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldNotSeeNodeThatHasAPropertyRemovedInTransaction() throws Exception
    {
        long nodeID = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            ktx.dataWrite().nodeRemoveProperty( nodeID, index.schema().getPropertyIds()[0] );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldSeeAllNodesAddedInTransaction() throws Exception
    {
        if ( index.type() != UNIQUE ) // this test does not make any sense for UNIQUE indexes
        {
            try ( Transaction ignore = graphDatabaseAPI.beginTx() )
            {
                long nodeID1 = createNode();
                long nodeID2 = createNode();
                long nodeID3 = createNode();
                KernelTransaction ktx = ktx();
                Set<Long> result = new HashSet<>(  );
                try ( NodeValueIndexCursor cursor = seek( ktx ) )
                {
                    while ( cursor.next() )
                    {
                        result.add( cursor.nodeReference() );
                    }
                }
                assertThat( result, containsInAnyOrder( nodeID1, nodeID2, nodeID3 ) );
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
            try ( Transaction ignore = graphDatabaseAPI.beginTx() )
            {
                KernelTransaction ktx = ktx();
                Set<Long> result = new HashSet<>(  );
                try ( NodeValueIndexCursor cursor = seek( ktx ) )
                {
                    while ( cursor.next() )
                    {
                        result.add( cursor.nodeReference() );
                    }
                }
                assertThat( result, containsInAnyOrder( nodeID1, nodeID2, nodeID3 ) );
            }
        }
    }

    @Test
    public void shouldNotSeeNodesLackingOneProperty() throws Exception
    {
        long nodeID1 = createNode();
        try ( Transaction ignore = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            Write write = ktx.dataWrite();
            long irrelevantNodeID = write.nodeCreate();
            write.nodeAddLabel( irrelevantNodeID, LABEL_ID );
            int[] propertyIds = index.schema().getPropertyIds();
            for ( int i = 0; i < propertyIds.length - 1; i++ )
            {
                int propID = propertyIds[i];
                write.nodeSetProperty( irrelevantNodeID, propID, Values.intValue( propID ) );
            }
            Set<Long> result = new HashSet<>(  );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                while ( cursor.next() )
                {
                    result.add( cursor.nodeReference() );
                }
            }
            assertThat( result, contains( nodeID1 ) );
        }
    }

    private long createNode()
            throws KernelException
    {
        long nodeID;
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ktx();
            Write write = ktx.dataWrite();
            nodeID = write.nodeCreate();
            write.nodeAddLabel( nodeID, LABEL_ID );
            for ( int propID : index.schema().getPropertyIds() )
            {
                write.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            tx.success();
        }
        return nodeID;
    }

    private NodeValueIndexCursor seek( KernelTransaction transaction ) throws KernelException
    {
        NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
        transaction.dataRead().nodeIndexSeek( fromDescriptor( index ), cursor, IndexOrder.NONE, exactQuery() );
        return cursor;
    }

    private IndexQuery[] exactQuery()
    {
        int[] propertyIds = index.schema().getPropertyIds();
        IndexQuery[] query = new IndexQuery[propertyIds.length];
        for ( int i = 0; i < query.length; i++ )
        {
            int propID = propertyIds[i];
            query[i] = IndexQuery.exact( propID, Values.of( propID ) );
        }
        return query;
    }

    private KernelTransaction ktx()
    {
        return graphDatabaseAPI.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread( true );
    }
}
