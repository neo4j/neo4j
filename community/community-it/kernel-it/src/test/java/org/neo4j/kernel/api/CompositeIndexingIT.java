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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

@ImpermanentDbmsExtension
@Timeout( 20 )
class CompositeIndexingIT
{
    private static final int LABEL_ID = 1;

    @Inject
    private GraphDatabaseAPI graphDatabaseAPI;
    private IndexDescriptor index;

    void setup( IndexPrototype prototype ) throws Exception
    {
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenWrite tokenWrite = ktx.tokenWrite();
            tokenWrite.labelGetOrCreateForName( "Label0" );
            assertEquals( LABEL_ID, tokenWrite.labelGetOrCreateForName( "Label1" ) );
            for ( int i = 0; i < 10; i++ )
            {
                tokenWrite.propertyKeyGetOrCreateForName( "prop" + i );
            }
            tx.commit();
        }
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            if ( prototype.isUnique() )
            {
                ConstraintDescriptor constraint = ktx.schemaWrite().uniquePropertyConstraintCreate( prototype );
                index = ktx.schemaRead().indexGetForName( constraint.getName() );
            }
            else
            {
                index = ktx.schemaWrite().indexCreate( prototype.schema(), null );
            }
            tx.commit();
        }

        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            while ( ktx.schemaRead().indexGetState( index ) != InternalIndexState.ONLINE )
            {
                Thread.sleep( 10 );
            } // Will break loop on test timeout
        }
    }

    @AfterEach
    void clean() throws Exception
    {
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            if ( index.isUnique() )
            {
                Iterator<ConstraintDescriptor> constraints = ktx.schemaRead().constraintsGetForSchema( index.schema() );
                while ( constraints.hasNext() )
                {
                    ktx.schemaWrite().constraintDrop( constraints.next() );
                }
            }
            else
            {
                ktx.schemaWrite().indexDrop( index );
            }
            tx.commit();
        }

        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            for ( Node node : tx.getAllNodes() )
            {
                node.delete();
            }
            tx.commit();
        }
    }

    private static Stream<Arguments> params()
    {
        return Stream.of(
            arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1 ) ) ),
            arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1, 2 ) ) ),
            arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1, 2, 3, 4 ) ) ),
            arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ) ),
            arguments( IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, 1 ) ) ),
            arguments( IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, 1, 2 ) ) ),
            arguments( IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeNodeAddedByPropertyToIndexInTranslation( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
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
                assertThat( cursor.nodeReference() ).isEqualTo( nodeID );
                assertFalse( cursor.next() );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeNodeAddedToByLabelIndexInTransaction( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
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
                assertThat( cursor.nodeReference() ).isEqualTo( nodeID );
                assertFalse( cursor.next() );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeNodeThatWasDeletedInTransaction( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        long nodeID = createNode();
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            ktx.dataWrite().nodeDelete( nodeID );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertFalse( cursor.next() );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeNodeThatHasItsLabelRemovedInTransaction( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        long nodeID = createNode();
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            ktx.dataWrite().nodeRemoveLabel( nodeID, LABEL_ID );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertFalse( cursor.next() );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeNodeThatHasAPropertyRemovedInTransaction( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        long nodeID = createNode();
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            ktx.dataWrite().nodeRemoveProperty( nodeID, index.schema().getPropertyIds()[0] );
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                assertFalse( cursor.next() );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeAllNodesAddedInTransaction( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        if ( !index.isUnique() ) // this test does not make any sense for UNIQUE indexes
        {
            long nodeID1 = createNode();
            long nodeID2 = createNode();
            long nodeID3 = createNode();
            try ( Transaction tx = graphDatabaseAPI.beginTx() )
            {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                Set<Long> result = new HashSet<>();
                try ( NodeValueIndexCursor cursor = seek( ktx ) )
                {
                    while ( cursor.next() )
                    {
                        result.add( cursor.nodeReference() );
                    }
                }
                assertThat( result ).contains( nodeID1, nodeID2, nodeID3 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeAllNodesAddedBeforeTransaction( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        if ( !index.isUnique() ) // this test does not make any sense for UNIQUE indexes
        {
            long nodeID1 = createNode();
            long nodeID2 = createNode();
            long nodeID3 = createNode();
            try ( Transaction tx = graphDatabaseAPI.beginTx() )
            {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
                Set<Long> result = new HashSet<>();
                try ( NodeValueIndexCursor cursor = seek( ktx ) )
                {
                    while ( cursor.next() )
                    {
                        result.add( cursor.nodeReference() );
                    }
                }
                assertThat( result ).contains( nodeID1, nodeID2, nodeID3 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeNodesLackingOneProperty( IndexPrototype prototype ) throws Exception
    {
        setup( prototype );
        long nodeID1 = createNode();
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            Write write = ktx.dataWrite();
            long irrelevantNodeID = write.nodeCreate();
            write.nodeAddLabel( irrelevantNodeID, LABEL_ID );
            int[] propertyIds = index.schema().getPropertyIds();
            for ( int i = 0; i < propertyIds.length - 1; i++ )
            {
                int propID = propertyIds[i];
                write.nodeSetProperty( irrelevantNodeID, propID, Values.intValue( propID ) );
            }
            Set<Long> result = new HashSet<>();
            try ( NodeValueIndexCursor cursor = seek( ktx ) )
            {
                while ( cursor.next() )
                {
                    result.add( cursor.nodeReference() );
                }
            }
            assertThat( result ).containsExactly( nodeID1 );
        }
    }

    private long createNode()
            throws KernelException
    {
        long nodeID;
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            Write write = ktx.dataWrite();
            nodeID = write.nodeCreate();
            write.nodeAddLabel( nodeID, LABEL_ID );
            for ( int propID : index.schema().getPropertyIds() )
            {
                write.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
            }
            tx.commit();
        }
        return nodeID;
    }

    private NodeValueIndexCursor seek( KernelTransaction transaction ) throws KernelException
    {
        NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
        IndexReadSession indexSession = transaction.dataRead().indexReadSession( index );
        transaction.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, exactQuery() );
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
}
