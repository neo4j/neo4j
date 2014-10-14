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
package org.neo4j.kernel.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode;
import org.neo4j.kernel.impl.api.state.RelationshipState;
import org.neo4j.kernel.impl.util.DiffSets;

/**
 * Kernel transaction state, please see {@link org.neo4j.kernel.impl.api.state.TxStateImpl} for details.
 *
 * The naming of methods in this class roughly follows the naming of {@link org.neo4j.kernel.api.Statement}
 * with one exception: All transaction state mutators must include the particle "Do" in their name, e.g.
 * nodeDoAdd. This helps deciding where to set "hasChanges" in the main implementation class {@link org.neo4j.kernel.impl.api.state.TxStateImpl}.
 */
public interface TxState
{
    public enum UpdateTriState
    {
        ADDED
        {
            @Override
            public boolean isTouched()
            {
                return true;
            }

            @Override
            public boolean isAdded()
            {
                return true;
            }
        },
        REMOVED
        {
            @Override
            public boolean isTouched()
            {
                return true;
            }

            @Override
            public boolean isAdded()
            {
                return false;
            }
        },
        UNTOUCHED
        {
            @Override
            public boolean isTouched()
            {
                return false;
            }

            @Override
            public boolean isAdded()
            {
                throw new UnsupportedOperationException( "Cannot convert an UNTOUCHED UpdateTriState to a boolean" );
            }
        };

        public abstract boolean isTouched();

        public abstract boolean isAdded();
    }

    public interface Holder
    {

        TxState txState();
        boolean hasTxState();

        boolean hasTxStateWithChanges();

    }

    public interface Visitor
    {
        void visitCreatedNode( long id );

        void visitDeletedNode( long id );

        void visitCreatedRelationship( long id, int type, long startNode, long endNode );

        void visitDeletedRelationship( long id, int type, long startNode, long endNode );

        void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                       Iterator<Integer> removed );

        void visitNodeRelationshipChanges( long id, RelationshipChangesForNode added,
                RelationshipChangesForNode removed );

        void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                      Iterator<Integer> removed );

        void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                        Iterator<Integer> removed );

        void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed );

        void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

        void visitAddedConstraint( UniquenessConstraint element );

        void visitRemovedConstraint( UniquenessConstraint element );

        void visitCreatedLabelToken( String name, int id);

        void visitCreatedPropertyKeyToken( String name, int id);

        void visitCreatedRelationshipTypeToken( String name, int id);

        void visitCreatedNodeLegacyIndex( String name, Map<String, String> config );

        void visitCreatedRelationshipLegacyIndex( String name, Map<String, String> config );

        void visitDeletedNodeLegacyIndex( String name );

        void visitDeletedRelationshipLegacyIndex( String name );
    }

    public static class VisitorAdapter implements Visitor
    {
        @Override
        public void visitCreatedNode( long id )
        {   // Ignore
        }

        @Override
        public void visitDeletedNode( long id )
        {   // Ignore
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
        {   // Ignore
        }

        @Override
        public void visitDeletedRelationship( long id, int type, long startNode, long endNode )
        {   // Ignore
        }

        @Override
        public void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed, Iterator<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitNodeRelationshipChanges( long id, RelationshipChangesForNode added,
                RelationshipChangesForNode removed )
        {   // Ignore
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed, Iterator<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                Iterator<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {   // Ignore
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {   // Ignore
        }

        @Override
        public void visitAddedConstraint( UniquenessConstraint element )
        {   // Ignore
        }

        @Override
        public void visitRemovedConstraint( UniquenessConstraint element )
        {   // Ignore
        }

        @Override
        public void visitCreatedLabelToken( String name, int id )
        {   // Ignore
        }

        @Override
        public void visitCreatedPropertyKeyToken( String name, int id )
        {   // Ignore
        }

        @Override
        public void visitCreatedRelationshipTypeToken( String name, int id )
        {   // Ignore
        }

        @Override
        public void visitCreatedNodeLegacyIndex( String name, Map<String, String> config )
        {   // Ignore
        }

        @Override
        public void visitCreatedRelationshipLegacyIndex( String name, Map<String, String> config )
        {   // Ignore
        }

        @Override
        public void visitDeletedNodeLegacyIndex( String name )
        {   // Ignore
        }

        @Override
        public void visitDeletedRelationshipLegacyIndex( String name )
        {   // Ignore
        }
    }

    boolean hasChanges();

    void accept( Visitor visitor );


    // ENTITY RELATED

    void relationshipDoCreate( long id, int relationshipTypeId, long startNodeId, long endNodeId );

    void nodeDoCreate( long id );

    DiffSets<Long> labelStateNodeDiffSets( int labelId );

    DiffSets<Integer> nodeStateLabelDiffSets( long nodeId );

    Iterator<DefinedProperty> augmentNodeProperties( long nodeId, Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> augmentRelProperties( long relId, Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> augmentGraphProperties( Iterator<DefinedProperty> original );

    Iterator<DefinedProperty> addedAndChangedNodeProperties( long nodeId );

    Iterator<DefinedProperty> addedAndChangedRelProperties( long relId );

    /** Returns all nodes that, in this tx, have had labelId added. */
    Set<Long> nodesWithLabelAdded( int labelId );

    /** Returns all nodes that, in this tx, have had labelId removed.  */
    DiffSets<Long> nodesWithLabelChanged( int labelId );

    /** Returns nodes that have been added and removed in this tx. */
    DiffSets<Long> addedAndRemovedNodes();

    /** Returns rels that have been added and removed in this tx. */
    DiffSets<Long> addedAndRemovedRels();

    /** Nodes that have had labels, relationships, or properties modified in this tx. */
    Iterable<NodeState> modifiedNodes();

    /** Rels that have properties modified in this tx. */
    Iterable<RelationshipState> modifiedRelationships();

    boolean nodeIsAddedInThisTx( long nodeId );

    boolean nodeIsDeletedInThisTx( long nodeId );

    boolean nodeModifiedInThisTx( long nodeId );

    DiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value );

    boolean relationshipIsAddedInThisTx( long relationshipId );

    boolean relationshipIsDeletedInThisTx( long relationshipId );

    UpdateTriState labelState( long nodeId, int labelId );

    void relationshipDoDelete( long relationshipId, int type, long startNode, long endNode );

    void relationshipDoDeleteAddedInThisTx( long relationshipId );

    void nodeDoDelete( long nodeId );

    void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty );

    void relationshipDoReplaceProperty( long relationshipId,
                                        Property replacedProperty, DefinedProperty newProperty );

    void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty );

    void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty );

    void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty );

    void graphDoRemoveProperty( DefinedProperty removedProperty );

    void nodeDoAddLabel( int labelId, long nodeId );

    void nodeDoRemoveLabel( int labelId, long nodeId );

    void labelCreateForName( String labelName, int id );

    void propertyKeyCreateForName( String propertyKeyName, int id );

    void relationshipTypeCreateForName( String relationshipTypeName, int id );

    PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, PrimitiveLongIterator stored );

    PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, int[] relTypes,
                                                PrimitiveLongIterator stored );

    PrimitiveLongIterator addedRelationships( long nodeId, int[] relTypes, Direction direction );

    int augmentNodeDegree( long node, int committedDegree, Direction direction );

    int augmentNodeDegree( long node, int committedDegree, Direction direction, int relType );

    PrimitiveIntIterator nodeRelationshipTypes( long nodeId );

    // SCHEMA RELATED

    DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId );

    DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId );

    DiffSets<IndexDescriptor> indexChanges();

    DiffSets<IndexDescriptor> constraintIndexChanges();

    Iterable<IndexDescriptor> constraintIndexesCreatedInTx();

    DiffSets<UniquenessConstraint> constraintsChanges();

    DiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId );

    DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId,
                                                                          int propertyKey );

    void indexRuleDoAdd( IndexDescriptor descriptor );

    void constraintIndexRuleDoAdd( IndexDescriptor descriptor );

    void indexDoDrop( IndexDescriptor descriptor );

    void constraintIndexDoDrop( IndexDescriptor descriptor );

    void constraintDoAdd( UniquenessConstraint constraint, long indexId );

    void constraintDoDrop( UniquenessConstraint constraint );

    boolean constraintDoUnRemove( UniquenessConstraint constraint );

    boolean constraintIndexDoUnRemove( IndexDescriptor index );

    Long indexCreatedForConstraint( UniquenessConstraint constraint );

    PrimitiveLongIterator augmentNodesGetAll( PrimitiveLongIterator committed );

    PrimitiveLongIterator augmentRelationshipsGetAll( PrimitiveLongIterator committed );

    /**
     * @return {@code true} if the relationship was visited in this state, i.e. if it was created
     * by this current transaction, otherwise {@code false} where the relationship might need to be
     * visited from the store.
     */
    <EXCEPTION extends Exception> boolean relationshipVisit(
            long relId, RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION;

    // <Legacy index>
    void nodeLegacyIndexDoCreate( String indexName, Map<String, String> customConfig );

    void relationshipLegacyIndexDoCreate( String indexName, Map<String, String> customConfig );

    void nodeLegacyIndexDoDelete( String indexName ) throws LegacyIndexNotFoundKernelException;

    void relationshipLegacyIndexDoDelete( String indexName ) throws LegacyIndexNotFoundKernelException;

    LegacyIndex getNodeLegacyIndexChanges( String indexName ) throws LegacyIndexNotFoundKernelException;

    LegacyIndex getRelationshipLegacyIndexChanges( String indexName ) throws LegacyIndexNotFoundKernelException;
    // </Legacy index>

    DiffSets<Long> indexUpdates( IndexDescriptor index, Object value );

    void indexUpdateProperty( IndexDescriptor descriptor, long nodeId, DefinedProperty before, DefinedProperty after );
}
