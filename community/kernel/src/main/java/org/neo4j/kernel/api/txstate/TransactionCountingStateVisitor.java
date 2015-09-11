package org.neo4j.kernel.api.txstate;

import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.DegreeItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.CountsRecordState;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;

import static org.neo4j.kernel.api.CountsRead.ANY_LABEL;
import static org.neo4j.kernel.api.CountsRead.ANY_RELATIONSHIP_TYPE;

public class TransactionCountingStateVisitor extends TxStateVisitor.Adapter
{
    private final RelationshipDataExtractor edge = new RelationshipDataExtractor();
    private final CountsRecordState counts;
    private final StoreReadLayer storeLayer;
    private final EntityReadOperations operations;
    private final TxStateHolder txStateHolder;

    public TransactionCountingStateVisitor( TxStateVisitor next, StoreReadLayer storeLayer,
            EntityReadOperations operations, TxStateHolder txStateHolder,
            CountsRecordState counts )
    {
        super( next );
        this.storeLayer = storeLayer;
        this.operations = operations;
        this.txStateHolder = txStateHolder;
        this.counts = counts;
    }

    @Override
    public void visitCreatedNode( long id )
    {
        counts.incrementNodeCount( ANY_LABEL, 1 );
        super.visitCreatedNode( id );
    }

    @Override
    public void visitDeletedNode( long id )
    {
        try ( StoreStatement statement = storeLayer.acquireStatement() )
        {
            counts.incrementNodeCount( ANY_LABEL, -1 );
            try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( id ) )
            {
                if ( node.next() )
                {
                    // TODO Rewrite this to use cursors directly instead of iterator
                    PrimitiveIntIterator labels = node.get().getLabels();
                    if ( labels.hasNext() )
                    {
                        final int[] removed = PrimitiveIntCollections.asArray( labels );
                        for ( int label : removed )
                        {
                            counts.incrementNodeCount( label, -1 );
                        }

                        try ( Cursor<DegreeItem> degrees = node.get().degrees() )
                        {
                            while ( degrees.next() )
                            {
                                DegreeItem degree = degrees.get();
                                for ( int label : removed )
                                {
                                    updateRelationshipsCountsFromDegrees( degree.type(), label, -degree.outgoing(),
                                            -degree.incoming() );
                                }
                            }
                        }
                    }
                }
            }
        }
        super.visitDeletedNode( id );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            throws ConstraintValidationKernelException
    {
        try
        {
            updateRelationshipCount( startNode, type, endNode, 1 );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( "Nodes with added relationships should exist.", e );
        }
        super.visitCreatedRelationship( id, type, startNode, endNode );
    }

    @Override
    public void visitDeletedRelationship( long id )
    {
        try
        {
            storeLayer.relationshipVisit( id, edge );
            updateRelationshipCount( edge.startNode(), edge.type(), edge.endNode(), -1 );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException(
                    "Relationship being deleted should exist along with its nodes.", e );
        }
        super.visitDeletedRelationship( id );
    }

    @Override
    public void visitNodeLabelChanges( long id, final Set<Integer> added, final Set<Integer> removed )
            throws ConstraintValidationKernelException
    {
        try ( StoreStatement statement = storeLayer.acquireStatement() )
        {
            // update counts
            if ( !(added.isEmpty() && removed.isEmpty()) )
            {
                for ( Integer label : added )
                {
                    counts.incrementNodeCount( label, 1 );
                }
                for ( Integer label : removed )
                {
                    counts.incrementNodeCount( label, -1 );
                }
                // get the relationship counts from *before* this transaction,
                // the relationship changes will compensate for what happens during the transaction
                try ( Cursor<NodeItem> node = statement.acquireSingleNodeCursor( id ) )
                {
                    if ( node.next() )
                    {
                        try ( Cursor<DegreeItem> degrees = node.get().degrees() )
                        {
                            while ( degrees.next() )
                            {
                                DegreeItem degree = degrees.get();

                                for ( Integer label : added )
                                {
                                    updateRelationshipsCountsFromDegrees( degree.type(), label, degree.outgoing(),
                                            degree.incoming() );
                                }
                                for ( Integer label : removed )
                                {
                                    updateRelationshipsCountsFromDegrees( degree.type(), label, -degree.outgoing(),
                                            -degree.incoming() );
                                }
                            }
                        }
                    }
                }
            }
        }
        super.visitNodeLabelChanges( id, added, removed );
    }

    private void updateRelationshipsCountsFromDegrees( int type, int label, long outgoing, long incoming )
    {
        // untyped
        counts.incrementRelationshipCount( label, ANY_RELATIONSHIP_TYPE, ANY_LABEL, outgoing );
        counts.incrementRelationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, label, incoming );
        // typed
        counts.incrementRelationshipCount( label, type, ANY_LABEL, outgoing );
        counts.incrementRelationshipCount( ANY_LABEL, type, label, incoming );
    }

    private void updateRelationshipCount( long startNode, int type, long endNode, int delta )
            throws EntityNotFoundException
    {
        updateRelationshipsCountsFromDegrees( type, ANY_LABEL, delta, 0 );
        for ( PrimitiveIntIterator startLabels = labelsOf( startNode ); startLabels.hasNext(); )
        {
            updateRelationshipsCountsFromDegrees( type, startLabels.next(), delta, 0 );
        }
        for ( PrimitiveIntIterator endLabels = labelsOf( endNode ); endLabels.hasNext(); )
        {
            updateRelationshipsCountsFromDegrees( type, endLabels.next(), 0, delta );
        }
    }

    private PrimitiveIntIterator labelsOf( long nodeId )
    {
        try ( StoreStatement statement = storeLayer.acquireStatement() )
        {
            try ( Cursor<NodeItem> node = operations.nodeCursor( txStateHolder, statement, nodeId ) )
            {
                if ( node.next() )
                {
                    return node.get().getLabels();
                }
                else
                {
                    return PrimitiveIntCollections.emptyIterator();
                }
            }
        }
    }
}
