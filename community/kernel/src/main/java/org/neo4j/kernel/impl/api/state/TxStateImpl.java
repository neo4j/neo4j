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
package org.neo4j.kernel.impl.api.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.LegacyIndex;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.util.DiffSets;

import static org.neo4j.helpers.collection.Iterables.map;

/**
 * This class contains transaction-local changes to the graph. These changes can then be used to augment reads from the
 * committed state of the database (to make the local changes appear in local transaction read operations). At commit
 * time a visitor is sent into this class to convert the end result of the tx changes into a physical changeset.
 *
 * See {@link org.neo4j.kernel.impl.api.KernelTransactionImplementation} for how this happens.
 *
 * This class is very large, as it has been used as a gathering point to consolidate all transaction state knowledge
 * into one component. Now that that work is done, this class should be refactored to increase transparency in how it
 * works.
 */
public final class TxStateImpl implements TxState
{
    private static final StateCreator<LabelState> LABEL_STATE_CREATOR = new StateCreator<LabelState>()
    {
        @Override
        public LabelState newState( long id )
        {
            return new LabelState( id );
        }
    };

    private static final StateCreator<NodeState> NODE_STATE_CREATOR = new StateCreator<NodeState>()
    {
        @Override
        public NodeState newState( long id )
        {
            return new NodeState( id );
        }
    };

    private static final StateCreator<RelationshipState> RELATIONSHIP_STATE_CREATOR =
            new StateCreator<RelationshipState>()
    {
        @Override
        public RelationshipState newState( long id )
        {
            return new RelationshipState( id );
        }
    };

    private Map<Long/*Node ID*/, NodeState> nodeStatesMap;
    private Map<Long/*Relationship ID*/, RelationshipState> relationshipStatesMap;
    private Map<Long/*Label ID*/, LabelState> labelStatesMap;

    private Map<Integer/*Token ID*/,String> createdLabelTokens;
    private Map<Integer/*Token ID*/,String> createdPropertyKeyTokens;
    private Map<Integer/*Token ID*/,String> createdRelationshipTypeTokens;

    private GraphState graphState;
    private DiffSets<IndexDescriptor> indexChanges;
    private DiffSets<IndexDescriptor> constraintIndexChanges;
    private DiffSets<UniquenessConstraint> constraintsChanges;

    private PropertyChanges propertyChangesForNodes;

    // Tracks added and removed nodes, not modified nodes
    private DiffSets<Long> nodes;

    // Tracks added and removed relationships, not modified relationships
    private DiffSets<Long> relationships;

    // This is temporary. It is needed until we've removed nodes and rels from the global cache, to tell
    // that they were created and then deleted in the same tx. This is here just to set a save point to
    // get a large set of changes in, and is meant to be removed in the coming days in a follow-up commit.
    private final Set<Long> nodesCreatedAndDeletedInTx = new HashSet<>();
    private final Set<Long> relsCreatedAndDeletedInTx = new HashSet<>();

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint;

    private Map<String, Map<String, String>> createdNodeLegacyIndexes;
    private Map<String, Map<String, String>> createdRelationshipLegacyIndexes;
    private Set<String> deletedNodeLegacyIndexes;
    private Set<String> deletedRelationshipLegacyIndexes;

    private final LegacyIndexTransactionState legacyIndexState;
    private Map<String, LegacyIndex> nodeLegacyIndexChanges;
    private Map<String, LegacyIndex> relationshipLegacyIndexChanges;
    private PrimitiveIntObjectMap<Map<DefinedProperty, DiffSets<Long>>> indexUpdates;

    private boolean hasChanges;

    public TxStateImpl( LegacyIndexTransactionState legacyIndexState )
    {
        this.legacyIndexState = legacyIndexState;
    }

    @Override
    public void accept( final Visitor visitor )
    {
        // Created nodes
        if ( nodes != null )
        {
            nodes.accept( new DiffSets.VisitorAdapter<Long>()
            {
                @Override
                public void visitAdded( Long element )
                {
                    visitor.visitCreatedNode( element.longValue() );
                }
            } );
        }

        // Created relationships
        if ( relationships != null )
        {
            relationships.accept( new DiffSets.VisitorAdapter<Long>()
            {
                @Override
                public void visitAdded( Long element )
                {
                    // It's fine to call "getOrCreate" here since we only get callbacks for relationships
                    // that have been added to this state map.
                    RelationshipState relationshipState = getOrCreateRelationshipState( element.longValue() );
                    visitor.visitCreatedRelationship( element.longValue(), relationshipState.type(),
                            relationshipState.startNode(), relationshipState.endNode() );
                }
            } );
        }

        // Deleted relationships
        if ( relationships != null )
        {
            relationships.accept( new DiffSets.VisitorAdapter<Long>()
            {
                @Override
                public void visitRemoved( Long element )
                {
                    RelationshipState relationshipState = getOrCreateRelationshipState( element.longValue() );
                    visitor.visitDeletedRelationship( element.longValue(), relationshipState.type(),
                            relationshipState.startNode(), relationshipState.endNode() );
                }
            } );
        }

        // Deleted nodes
        if ( nodes != null )
        {
            nodes.accept( new DiffSets.VisitorAdapter<Long>()
            {
                @Override
                public void visitRemoved( Long element )
                {
                    visitor.visitDeletedNode( element.longValue() );
                }
            } );
        }

        if ( hasNodeStatesMap() && !nodeStatesMap().isEmpty() )
        {
            for ( NodeState node : modifiedNodes() )
            {
                node.accept( nodeVisitor( visitor ) );
            }
        }

        if ( hasRelationshipsStatesMap() && !relationshipStatesMap().isEmpty() )
        {
            for ( RelationshipState rel : modifiedRelationships() )
            {
                rel.accept( relVisitor( visitor ) );
            }
        }

        if( graphState != null )
        {
            graphState.accept( graphPropertyVisitor( visitor ) );
        }

        if ( hasIndexChangesDiffSets() && !indexChanges().isEmpty() )
        {
            indexChanges().accept( indexVisitor( visitor, false ) );
        }

        if ( hasConstraintIndexChangesDiffSets() && !constraintIndexChanges().isEmpty() )
        {
            constraintIndexChanges().accept( indexVisitor( visitor, true ) );
        }

        if ( hasConstraintsChangesDiffSets() && !constraintsChanges().isEmpty() )
        {
            constraintsChanges().accept( new DiffSets.Visitor<UniquenessConstraint>()
            {
                @Override
                public void visitAdded( UniquenessConstraint element )
                {
                    visitor.visitAddedConstraint( element );
                }

                @Override
                public void visitRemoved( UniquenessConstraint element )
                {
                    visitor.visitRemovedConstraint( element );
                }
            } );
        }

        if (createdLabelTokens != null)
        {
            for ( Map.Entry<Integer, String> entry : createdLabelTokens.entrySet() )
            {
                visitor.visitCreatedLabelToken( entry.getValue(), entry.getKey() );
            }
        }

        if (createdPropertyKeyTokens != null)
        {
            for ( Map.Entry<Integer, String> entry : createdPropertyKeyTokens.entrySet() )
            {
                visitor.visitCreatedPropertyKeyToken( entry.getValue(), entry.getKey() );
            }
        }

        if (createdRelationshipTypeTokens != null)
        {
            for ( Map.Entry<Integer, String> entry : createdRelationshipTypeTokens.entrySet() )
            {
                visitor.visitCreatedRelationshipTypeToken( entry.getValue(), entry.getKey() );
            }
        }

        if (createdNodeLegacyIndexes != null)
        {
            for ( Map.Entry<String, Map<String, String>> entry : createdNodeLegacyIndexes.entrySet() )
            {
                visitor.visitCreatedNodeLegacyIndex(entry.getKey(), entry.getValue());
            }
        }

        if (createdRelationshipLegacyIndexes != null)
        {
            for ( Map.Entry<String, Map<String, String>> entry : createdRelationshipLegacyIndexes.entrySet() )
            {
                visitor.visitCreatedRelationshipLegacyIndex(entry.getKey(), entry.getValue());
            }
        }

        if ( deletedNodeLegacyIndexes != null )
        {
            for ( String index : deletedNodeLegacyIndexes )
            {
                visitor.visitDeletedNodeLegacyIndex( index );
            }
        }

        if ( deletedRelationshipLegacyIndexes != null )
        {
            for ( String index : deletedRelationshipLegacyIndexes )
            {
                visitor.visitDeletedRelationshipLegacyIndex( index );
            }
        }
    }

    private static DiffSets.Visitor<IndexDescriptor> indexVisitor( final Visitor visitor, final boolean forConstraint )
    {
        return new DiffSets.Visitor<IndexDescriptor>()
        {
            @Override
            public void visitAdded( IndexDescriptor element )
            {
                visitor.visitAddedIndex( element, forConstraint );
            }

            @Override
            public void visitRemoved( IndexDescriptor element )
            {
                visitor.visitRemovedIndex( element, forConstraint );
            }
        };
    }

    private static NodeState.Visitor nodeVisitor( final Visitor visitor )
    {
        return new NodeState.Visitor()
        {
            @Override
            public void visitLabelChanges( long nodeId, Set<Integer> added, Set<Integer> removed )
            {
                visitor.visitNodeLabelChanges( nodeId, added, removed );
            }

            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitNodePropertyChanges( entityId, added, changed, removed );
            }

            @Override
            public void visitRelationshipChanges( long nodeId, RelationshipChangesForNode added,
                    RelationshipChangesForNode removed )
            {
                visitor.visitNodeRelationshipChanges( nodeId, added, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor relVisitor( final Visitor visitor  )
    {
        return new PropertyContainerState.Visitor()
        {
            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitRelPropertyChanges( entityId, added, changed, removed );
            }
        };
    }

    private static PropertyContainerState.Visitor graphPropertyVisitor( final Visitor visitor  )
    {
        return new PropertyContainerState.Visitor()
        {
            @Override
            public void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added,
                                              Iterator<DefinedProperty> changed, Iterator<Integer> removed)
            {
                visitor.visitGraphPropertyChanges( added, changed, removed );
            }
        };
    }

    @Override
    public boolean hasChanges()
    {
        return hasChanges;
    }

    @Override
    public Iterable<NodeState> modifiedNodes()
    {
        return hasNodeStatesMap() ? nodeStatesMap().values() : Iterables.<NodeState>empty();
    }

    @Override
    public DiffSets<Long> labelStateNodeDiffSets( int labelId )
    {
        return getOrCreateLabelState( labelId ).getNodeDiffSets();
    }

    @Override
    public DiffSets<Integer> nodeStateLabelDiffSets( long nodeId )
    {
        return getOrCreateNodeState( nodeId ).labelDiffSets();
    }

    @Override
    public Iterator<DefinedProperty> augmentNodeProperties( long nodeId, Iterator<DefinedProperty> original )
    {
        NodeState state;
        if(nodeStatesMap != null && (state = nodeStatesMap.get( nodeId )) != null)
        {
            return state.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> augmentRelProperties( long relId, Iterator<DefinedProperty> original )
    {
        RelationshipState state;
        if(relationshipStatesMap != null && (state = relationshipStatesMap.get( relId )) != null)
        {
            return state.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> augmentGraphProperties( Iterator<DefinedProperty> original )
    {
        if(graphState != null)
        {
            return graphState.augmentProperties( original );
        }
        return original;
    }

    @Override
    public Iterator<DefinedProperty> addedAndChangedNodeProperties( long nodeId )
    {
        NodeState state;
        if(nodeStatesMap != null && (state = nodeStatesMap.get( nodeId )) != null)
        {
            return state.addedAndChangedProperties();
        }
        return IteratorUtil.emptyIterator();
    }

    @Override
    public Iterator<DefinedProperty> addedAndChangedRelProperties( long relId )
    {
        RelationshipState state;
        if(relationshipStatesMap != null && (state = relationshipStatesMap.get( relId )) != null)
        {
            return state.addedAndChangedProperties();
        }
        return IteratorUtil.emptyIterator();
    }

    @Override
    public boolean nodeIsAddedInThisTx( long nodeId )
    {
        return hasNodesAddedOrRemoved() && nodes.isAdded( nodeId );
    }

    @Override
    public boolean relationshipIsAddedInThisTx( long relationshipId )
    {
        return hasRelsAddedOrRemoved() && relationships.isAdded( relationshipId );
    }

    @Override
    public void nodeDoCreate( long id )
    {
        addedAndRemovedNodes().add( id );
        hasChanges = true;
    }

    @Override
    public void nodeDoDelete( long nodeId )
    {
        if ( addedAndRemovedNodes().remove( nodeId ) )
        {
            nodesCreatedAndDeletedInTx.add(nodeId);
        }

        if ( hasNodeStatesMap() )
        {
            NodeState nodeState = nodeStatesMap.remove( nodeId );
            if ( nodeState != null )
            {
                DiffSets<Integer> diff = nodeState.labelDiffSets();
                for ( Integer label : diff.getAdded() )
                {
                    labelStateNodeDiffSets( label ).remove( nodeId );
                }
                nodeState.clearIndexDiffs( nodeId );
                nodeState.clear();
            }
        }
        hasChanges = true;
    }

    @Override
    public void relationshipDoCreate( long id, int relationshipTypeId, long startNodeId, long endNodeId )
    {
        addedAndRemovedRels().add( id );

        if ( startNodeId == endNodeId )
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).addRelationship( id, relationshipTypeId, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).addRelationship( id, relationshipTypeId, Direction.INCOMING );
        }

        getOrCreateRelationshipState( id ).setMetaData( startNodeId, endNodeId, relationshipTypeId );

        hasChanges = true;
    }

    @Override
    public boolean nodeIsDeletedInThisTx( long nodeId )
    {
        return hasNodesAddedOrRemoved() && addedAndRemovedNodes().isRemoved( nodeId )
                // Temporary until we've stopped adding nodes to the global cache during tx.
                || nodesCreatedAndDeletedInTx.contains( nodeId );
    }

    @Override
    public boolean nodeModifiedInThisTx( long nodeId )
    {
        return nodeIsAddedInThisTx( nodeId ) || nodeIsDeletedInThisTx( nodeId ) || hasNodeState( nodeId );
    }

    @Override
    public void relationshipDoDelete( long id, int type, long startNodeId, long endNodeId )
    {
        if ( addedAndRemovedRels().remove( id ) )
        {
            relsCreatedAndDeletedInTx.add( id );
        }

        if ( startNodeId == endNodeId )
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.BOTH );
        }
        else
        {
            getOrCreateNodeState( startNodeId ).removeRelationship( id, type, Direction.OUTGOING );
            getOrCreateNodeState( endNodeId ).removeRelationship( id, type, Direction.INCOMING );
        }

        if ( hasRelationshipsStatesMap() )
        {
            RelationshipState removed = relationshipStatesMap.remove( id );
            if ( removed != null )
            {
                removed.clear();
            }
        }

        hasChanges = true;
    }

    @Override
    public void relationshipDoDeleteAddedInThisTx( long relationshipId )
    {
        RelationshipState state = getOrCreateRelationshipState( relationshipId );
        relationshipDoDelete( relationshipId, state.type(), state.startNode(), state.endNode() );
    }

    @Override
    public boolean relationshipIsDeletedInThisTx( long relationshipId )
    {
        return hasDeletedRelationshipsDiffSets() && addedAndRemovedRels().isRemoved( relationshipId )
                // Temporary until we stop adding rels to the global cache during tx
                || relsCreatedAndDeletedInTx.contains( relationshipId );
    }

    @Override
    public void nodeDoReplaceProperty( long nodeId, Property replacedProperty, DefinedProperty newProperty )
    {
        if ( replacedProperty.isDefined() )
        {
            getOrCreateNodeState( nodeId ).changeProperty( newProperty );
            nodePropertyChanges().changeProperty( nodeId, replacedProperty.propertyKeyId(),
                    ((DefinedProperty)replacedProperty).value(), newProperty.value() );
        }
        else
        {
            NodeState nodeState = getOrCreateNodeState( nodeId );
//            nodeState.
            nodeState.addProperty( newProperty );
            nodePropertyChanges().addProperty(nodeId, newProperty.propertyKeyId(), newProperty.value());
        }
        hasChanges = true;
    }

    @Override
    public void relationshipDoReplaceProperty( long relationshipId, Property replacedProperty, DefinedProperty newProperty )
    {
        if(replacedProperty.isDefined())
        {
            getOrCreateRelationshipState( relationshipId ).changeProperty( newProperty );
        }
        else
        {
            getOrCreateRelationshipState( relationshipId ).addProperty( newProperty );
        }
        hasChanges = true;
    }

    @Override
    public void graphDoReplaceProperty( Property replacedProperty, DefinedProperty newProperty )
    {
        if(replacedProperty.isDefined())
        {
            getOrCreateGraphState().changeProperty( newProperty );
        }
        else
        {
            getOrCreateGraphState().addProperty( newProperty );
        }
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveProperty( long nodeId, DefinedProperty removedProperty )
    {
        getOrCreateNodeState( nodeId ).removeProperty( removedProperty );
        nodePropertyChanges().removeProperty( nodeId, removedProperty.propertyKeyId(),
                removedProperty.value() );
        hasChanges = true;
    }

    @Override
    public void relationshipDoRemoveProperty( long relationshipId, DefinedProperty removedProperty )
    {
        getOrCreateRelationshipState( relationshipId ).removeProperty( removedProperty );
        hasChanges = true;
    }

    @Override
    public void graphDoRemoveProperty( DefinedProperty removedProperty )
    {
        getOrCreateGraphState().removeProperty( removedProperty );
        hasChanges = true;
    }

    @Override
    public void nodeDoAddLabel( int labelId, long nodeId )
    {
        labelStateNodeDiffSets( labelId ).add( nodeId );
        nodeStateLabelDiffSets( nodeId ).add( labelId );
        hasChanges = true;
    }

    @Override
    public void nodeDoRemoveLabel( int labelId, long nodeId )
    {
        labelStateNodeDiffSets( labelId ).remove( nodeId );
        nodeStateLabelDiffSets( nodeId ).remove( labelId );
        hasChanges = true;
    }

    @Override
    public void labelCreateForName( String labelName, int id )
    {
        if (createdLabelTokens == null)
        {
            createdLabelTokens = new HashMap<>();
        }

        createdLabelTokens.put( id, labelName );

        hasChanges = true;
    }

    @Override
    public void propertyKeyCreateForName( String propertyKeyName, int id )
    {
        if (createdPropertyKeyTokens == null)
        {
            createdPropertyKeyTokens = new HashMap<>();
        }

        createdPropertyKeyTokens.put( id, propertyKeyName );

        hasChanges = true;
    }

    @Override
    public void relationshipTypeCreateForName( String labelName, int id )
    {
        if (createdRelationshipTypeTokens == null)
        {
            createdRelationshipTypeTokens = new HashMap<>();
        }

        createdRelationshipTypeTokens.put( id, labelName );

        hasChanges = true;
    }

    @Override
    public UpdateTriState labelState( long nodeId, int labelId )
    {
        NodeState nodeState = getState( nodeStatesMap(), nodeId, null );
        if ( nodeState != null )
        {
            DiffSets<Integer> labelDiff = nodeState.labelDiffSets();
            if ( labelDiff.isAdded( labelId ) )
            {
                return UpdateTriState.ADDED;
            }
            if ( labelDiff.isRemoved( labelId ) )
            {
                return UpdateTriState.REMOVED;
            }
        }
        return UpdateTriState.UNTOUCHED;
    }

    @Override
    public Set<Long> nodesWithLabelAdded( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState state = getState( labelStatesMap, labelId, null );
            if ( null != state )
            {
                return state.getNodeDiffSets().getAdded();
            }
        }

        return Collections.emptySet();
    }

    @Override
    public DiffSets<Long> nodesWithLabelChanged( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState state = getState( labelStatesMap, labelId, null );
            if ( null != state )
            {
                return state.getNodeDiffSets();
            }
        }
        return DiffSets.emptyDiffSets();
    }

    @Override
    public void indexRuleDoAdd( IndexDescriptor descriptor )
    {
        DiffSets<IndexDescriptor> diff = indexChanges();
        if ( diff.unRemove( descriptor ) )
        {
            getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().unRemove( descriptor );
        }
        else
        {
            indexChanges().add( descriptor );
            getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().add( descriptor );
        }
        hasChanges = true;
    }

    @Override
    public void constraintIndexRuleDoAdd( IndexDescriptor descriptor )
    {
        constraintIndexChanges().add( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).constraintIndexChanges().add( descriptor );
        hasChanges = true;
    }

    @Override
    public void indexDoDrop( IndexDescriptor descriptor )
    {
        indexChanges().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).indexChanges().remove( descriptor );
        hasChanges = true;
    }

    @Override
    public void constraintIndexDoDrop( IndexDescriptor descriptor )
    {
        constraintIndexChanges().remove( descriptor );
        getOrCreateLabelState( descriptor.getLabelId() ).constraintIndexChanges().remove( descriptor );
        hasChanges = true;
    }

    @Override
    public DiffSets<IndexDescriptor> indexDiffSetsByLabel( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState labelState = getState( labelStatesMap, labelId, null );
            if ( null != labelState )
            {
                return labelState.indexChanges();
            }
        }
        return DiffSets.emptyDiffSets();
    }

    @Override
    public DiffSets<IndexDescriptor> constraintIndexDiffSetsByLabel( int labelId )
    {
        if ( hasLabelStatesMap() )
        {
            LabelState labelState = getState( labelStatesMap(), labelId, null );
            if (labelState != null)
            {
                return labelState.constraintIndexChanges();
            }
        }
        return DiffSets.emptyDiffSets();
    }

    @Override
    public DiffSets<IndexDescriptor> indexChanges()
    {
        if ( !hasIndexChangesDiffSets() )
        {
            indexChanges = new DiffSets<>();
        }
        return indexChanges;
    }

    private boolean hasIndexChangesDiffSets()
    {
        return indexChanges != null;
    }

    @Override
    public DiffSets<IndexDescriptor> constraintIndexChanges()
    {
        if ( !hasConstraintIndexChangesDiffSets() )
        {
            constraintIndexChanges = new DiffSets<>();
        }
        return constraintIndexChanges;
    }

    private boolean hasConstraintIndexChangesDiffSets()
    {
        return constraintIndexChanges != null;
    }

    @Override
    public DiffSets<Long> nodesWithChangedProperty( int propertyKeyId, Object value )
    {
        return propertyChangesForNodes != null ? propertyChangesForNodes.changesForProperty( propertyKeyId, value ) :
                DiffSets.<Long>emptyDiffSets();
    }

    @Override
    public DiffSets<Long> addedAndRemovedNodes()
    {
        if ( !hasNodesAddedOrRemoved() )
        {
            nodes = new DiffSets<>();
        }
        return nodes;
    }

    private boolean hasNodesAddedOrRemoved()
    {
        return nodes != null;
    }

    private boolean hasRelsAddedOrRemoved()
    {
        return relationships != null;
    }

    @Override
    public PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, PrimitiveLongIterator rels )
    {
        if(hasNodeState( nodeId ))
        {
            rels = getOrCreateNodeState( nodeId ).augmentRelationships( direction, rels );
            // TODO: This should be handled by the augment call above
            if(hasDeletedRelationshipsDiffSets())
            {
                rels = addedAndRemovedRels().augmentWithRemovals( rels );
            }
        }
        return rels;
    }

    @Override
    public PrimitiveLongIterator augmentRelationships( long nodeId, Direction direction, int[] types, PrimitiveLongIterator rels )
    {
        if(hasNodeState( nodeId ))
        {
            rels = getOrCreateNodeState( nodeId ).augmentRelationships( direction, types, rels );
            // TODO: This should be handled by the augment call above
            if(hasDeletedRelationshipsDiffSets())
            {
                rels = addedAndRemovedRels().augmentWithRemovals( rels );
            }
        }
        return rels;
    }

    @Override
    public PrimitiveLongIterator addedRelationships( long nodeId, int[] types, Direction direction )
    {
        if(hasNodeState( nodeId ))
        {
            return getOrCreateNodeState( nodeId ).addedRelationships( direction, types );
        }
        return null;
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction )
    {
        if(hasNodeState( nodeId ))
        {
            return getOrCreateNodeState( nodeId ).augmentDegree( direction, degree );
        }
        return degree;
    }

    @Override
    public int augmentNodeDegree( long nodeId, int degree, Direction direction, int typeId )
    {
        if(hasNodeState( nodeId ))
        {
            return getOrCreateNodeState( nodeId ).augmentDegree( direction, degree, typeId );
        }
        return degree;
    }

    @Override
    public PrimitiveIntIterator nodeRelationshipTypes( long nodeId )
    {
        if ( hasNodeState( nodeId ) )
        {
            return getOrCreateNodeState( nodeId ).relationshipTypes();
        }
        return PrimitiveIntCollections.emptyIterator();
    }

    @Override
    public DiffSets<Long> addedAndRemovedRels()
    {
        if ( !hasDeletedRelationshipsDiffSets() )
        {
            relationships = new DiffSets<>();
        }
        return relationships;
    }

    @Override
    public Iterable<RelationshipState> modifiedRelationships()
    {
        return relationshipStatesMap != null ? relationshipStatesMap.values() : Iterables.<RelationshipState>empty();
    }

    private boolean hasDeletedRelationshipsDiffSets()
    {
        return relationships != null;
    }

    private LabelState getOrCreateLabelState( int labelId )
    {
        return getState( labelStatesMap(), labelId, LABEL_STATE_CREATOR );
    }

    private NodeState getOrCreateNodeState( long nodeId )
    {
        return getState( nodeStatesMap(), nodeId, NODE_STATE_CREATOR );
    }

    private RelationshipState getOrCreateRelationshipState( long relationshipId )
    {
        return getState( relationshipStatesMap(), relationshipId, RELATIONSHIP_STATE_CREATOR );
    }

    private GraphState getOrCreateGraphState()
    {
        if ( graphState == null )
        {
            graphState = new GraphState();
        }
        return graphState;
    }

    private interface StateCreator<STATE>
    {
        STATE newState( long id );
    }

    private <STATE> STATE getState( Map<Long, STATE> states, long id, StateCreator<STATE> creator )
    {
        STATE result = states.get( id );
        if ( result != null )
        {
            return result;
        }

        if ( creator != null )
        {
            result = creator.newState( id );
            states.put( id, result );
            hasChanges = true;
        }
        return result;
    }

    @Override
    public void constraintDoAdd( UniquenessConstraint constraint, long indexId )
    {
        constraintsChanges().add( constraint );
        createdConstraintIndexesByConstraint().put( constraint, indexId );
        getOrCreateLabelState( constraint.label() ).constraintsChanges().add( constraint );
        hasChanges = true;
    }


    @Override
    public DiffSets<UniquenessConstraint> constraintsChangesForLabelAndProperty( int labelId, final int propertyKey )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges().filterAdded( new Predicate<UniquenessConstraint>()
        {
            @Override
            public boolean accept( UniquenessConstraint item )
            {
                return item.propertyKeyId() == propertyKey;
            }
        } );
    }

    @Override
    public DiffSets<UniquenessConstraint> constraintsChangesForLabel( int labelId )
    {
        return getOrCreateLabelState( labelId ).constraintsChanges();
    }

    @Override
    public DiffSets<UniquenessConstraint> constraintsChanges()
    {
        if ( !hasConstraintsChangesDiffSets() )
        {
            constraintsChanges = new DiffSets<>();
        }
        return constraintsChanges;
    }

    private boolean hasConstraintsChangesDiffSets()
    {
        return constraintsChanges != null;
    }

    @Override
    public void constraintDoDrop( UniquenessConstraint constraint )
    {
        constraintsChanges().remove( constraint );

        constraintIndexDoDrop( new IndexDescriptor( constraint.label(), constraint.propertyKeyId() ));
        constraintsChangesForLabel( constraint.label() ).remove( constraint );
        hasChanges = true;
    }

    @Override
    public boolean constraintDoUnRemove( UniquenessConstraint constraint )
    {
        if ( constraintsChanges().unRemove( constraint ) )
        {
            constraintsChangesForLabel( constraint.label() ).unRemove( constraint );
            return true;
        }
        return false;
    }

    @Override
    public boolean constraintIndexDoUnRemove( IndexDescriptor index )
    {
        if ( constraintIndexChanges().unRemove( index ) )
        {
            constraintIndexDiffSetsByLabel( index.getLabelId() ).unRemove( index );
            return true;
        }
        return false;
    }

    @Override
    public Iterable<IndexDescriptor> constraintIndexesCreatedInTx()
    {
       if ( hasCreatedConstraintIndexesMap() )
       {
           Map<UniquenessConstraint, Long> constraintMap = createdConstraintIndexesByConstraint();
           if ( !constraintMap.isEmpty() )
           {
               return map( new Function<UniquenessConstraint, IndexDescriptor>()
               {
                   @Override
                   public IndexDescriptor apply( UniquenessConstraint constraint )
                   {
                       return new IndexDescriptor( constraint.label(), constraint.propertyKeyId() );
                   }
               }, constraintMap.keySet() );
           }
       }

       return Iterables.empty();
    }

    @Override
    public Long indexCreatedForConstraint( UniquenessConstraint constraint )
    {
        return createdConstraintIndexesByConstraint == null ? null :
                createdConstraintIndexesByConstraint.get( constraint );
    }

    @Override
    public DiffSets<Long> indexUpdates( IndexDescriptor descriptor, Object value )
    {
        DiffSets<Long> diffs = getIndexUpdates( descriptor.getLabelId(), false, Property.property( descriptor.getPropertyKeyId(), value ) );
        return diffs == null ? DiffSets.<Long>emptyDiffSets() : diffs;
    }

    @Override
    public void indexUpdateProperty( IndexDescriptor descriptor, long nodeId, DefinedProperty propertyBefore, DefinedProperty propertyAfter )
    {
        {
            DiffSets<Long> before = getIndexUpdates( descriptor.getLabelId(), true, propertyBefore );
            if ( before != null )
            {
                before.remove( nodeId );
                //if ( hasNodesAddedOrRemoved() && addedAndRemovedNodes().getAdded().contains( nodeId ) )
                {
                    if ( before.getRemoved().contains( nodeId ) )
                    {
                        getOrCreateNodeState( nodeId ).addIndexDiff( before );
                    }
                    else
                    {
                        getOrCreateNodeState( nodeId ).removeIndexDiff( before );
                    }
                }
            }
        }
        {
            DiffSets<Long> after = getIndexUpdates( descriptor.getLabelId(), true, propertyAfter );
            if ( after != null )
            {
                after.add( nodeId );
                //if ( hasNodesAddedOrRemoved() && addedAndRemovedNodes().getAdded().contains( nodeId ) )
                {
                    if ( after.getAdded().contains( nodeId ) )
                    {
                        getOrCreateNodeState( nodeId ).addIndexDiff( after );
                    }
                    else
                    {
                        getOrCreateNodeState( nodeId ).removeIndexDiff( after );
                    }
                }
            }
        }
    }

    private DiffSets<Long> getIndexUpdates( int label, boolean create, DefinedProperty property )
    {
        if ( property == null )
        {
            return null;
        }
        if ( indexUpdates == null )
        {
            if ( !create )
            {
                return null;
            }
            indexUpdates = Primitive.intObjectMap();
        }
        Map<DefinedProperty, DiffSets<Long>> updates = indexUpdates.get( label );
        if ( updates == null )
        {
            if ( !create )
            {
                return null;
            }
            indexUpdates.put( label, updates = new HashMap<>() );
        }
        DiffSets<Long> diffs = updates.get( property );
        if ( diffs == null && create )
        {
            updates.put( property, diffs = new DiffSets<>() );
        }
        return diffs;
    }

    private Map<UniquenessConstraint, Long> createdConstraintIndexesByConstraint()
    {
        if ( !hasCreatedConstraintIndexesMap() )
        {
            createdConstraintIndexesByConstraint = new HashMap<>();
        }
        return createdConstraintIndexesByConstraint;
    }

    private boolean hasCreatedConstraintIndexesMap()
    {
        return null != createdConstraintIndexesByConstraint;
    }

    private boolean hasNodeState(long nodeId)
    {
        return hasNodeStatesMap() && nodeStatesMap().containsKey( nodeId );
    }

    private Map<Long, NodeState> nodeStatesMap()
    {
        if ( !hasNodeStatesMap() )
        {
            nodeStatesMap = new HashMap<>();
        }
        return nodeStatesMap;
    }

    private boolean hasNodeStatesMap()
    {
        return null != nodeStatesMap;
    }

    private Map<Long, RelationshipState> relationshipStatesMap()
    {
        if ( !hasRelationshipsStatesMap() )
        {
            relationshipStatesMap = new HashMap<>();
        }
        return relationshipStatesMap;
    }

    private boolean hasRelationshipsStatesMap()
    {
        return null != relationshipStatesMap;
    }

    private Map<Long, LabelState> labelStatesMap()
    {
        if ( !hasLabelStatesMap() )
        {
            labelStatesMap = new HashMap<>();
        }
        return labelStatesMap;
    }

    private boolean hasLabelStatesMap()
    {
        return null != labelStatesMap;
    }

    private PropertyChanges nodePropertyChanges()
    {
        return propertyChangesForNodes == null ?
                propertyChangesForNodes = new PropertyChanges() : propertyChangesForNodes;
    }

    @Override
    public PrimitiveLongIterator augmentNodesGetAll( PrimitiveLongIterator committed )
    {
        if ( !hasChanges() )
        {
            return committed;
        }

        return addedAndRemovedNodes().augment( committed );
    }

    @Override
    public PrimitiveLongIterator augmentRelationshipsGetAll( PrimitiveLongIterator committed )
    {
/*
        if ( !hasChanges() )
        {
            return committed;
        }
*/

        return addedAndRemovedRels().augment( committed );
    }

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit(
            long relId, RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
    {
        if ( relationshipIsAddedInThisTx( relId ) )
        {
            RelationshipState relationship = relationshipStatesMap.get( relId );
            visitor.visit( relId, relationship.type(), relationship.startNode(), relationship.endNode() );
            return true;
        }
        return false;
    }

    @Override
    public void nodeLegacyIndexDoCreate( String indexName, Map<String, String> customConfig )
    {
        assert customConfig != null;

        if ( createdNodeLegacyIndexes == null )
        {
            createdNodeLegacyIndexes = new HashMap<>();
        }

        createdNodeLegacyIndexes.put(indexName, customConfig);

        hasChanges = true;
    }

    @Override
    public void relationshipLegacyIndexDoCreate( String indexName, Map<String, String> customConfig )
    {
        assert customConfig != null;

        if ( createdRelationshipLegacyIndexes == null )
        {
            createdRelationshipLegacyIndexes = new HashMap<>();
        }

        createdRelationshipLegacyIndexes.put(indexName, customConfig);

        hasChanges = true;
    }

    @Override
    public void nodeLegacyIndexDoDelete( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        getNodeLegacyIndexChanges( indexName ).drop();
        if ( deletedNodeLegacyIndexes == null )
        {
            deletedNodeLegacyIndexes = new HashSet<>();
        }
        deletedNodeLegacyIndexes.add( indexName );
        hasChanges = true;
    }

    @Override
    public void relationshipLegacyIndexDoDelete( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        getRelationshipLegacyIndexChanges( indexName ).drop();
        if ( deletedRelationshipLegacyIndexes == null )
        {
            deletedRelationshipLegacyIndexes = new HashSet<>();
        }
        deletedRelationshipLegacyIndexes.add( indexName );
        hasChanges = true;
    }

    @Override
    public LegacyIndex getNodeLegacyIndexChanges( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        if ( nodeLegacyIndexChanges == null )
        {
            nodeLegacyIndexChanges = new HashMap<>();
        }
        LegacyIndex changes = nodeLegacyIndexChanges.get( indexName );
        if ( changes == null )
        {
            nodeLegacyIndexChanges.put( indexName, changes = legacyIndexState.nodeChanges( indexName ) );
        }
        return changes;
    }

    @Override
    public LegacyIndex getRelationshipLegacyIndexChanges( String indexName ) throws LegacyIndexNotFoundKernelException
    {
        if ( relationshipLegacyIndexChanges == null )
        {
            relationshipLegacyIndexChanges = new HashMap<>();
        }
        LegacyIndex changes = relationshipLegacyIndexChanges.get( indexName );
        if ( changes == null )
        {
            relationshipLegacyIndexChanges.put( indexName,
                    changes = legacyIndexState.relationshipChanges( indexName ) );
        }
        return changes;
    }
}
