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
package org.neo4j.kernel.impl.api.state;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.UpdateTriState;

public class NodeStateImpl extends PropertyContainerStateImpl implements NodeState
{
    private DiffSets<Integer> labelDiffSets;
    private RelationshipChangesForNode relationshipsAdded;
    private RelationshipChangesForNode relationshipsRemoved;
    private Set<DiffSets<Long>> indexDiffs;
    private final TxState state;

    NodeStateImpl( long id, TxState state )
    {
        super( id );
        this.state = state;
    }

    @Override
    public ReadableDiffSets<Integer> labelDiffSets()
    {
        return ReadableDiffSets.Empty.ifNull( labelDiffSets );
    }

    public DiffSets<Integer> getOrCreateLabelDiffSets()
    {
        if ( null == labelDiffSets )
        {
            labelDiffSets = new DiffSets<>();
        }
        return labelDiffSets;
    }

    public void addRelationship( long relId, int typeId, Direction direction )
    {
        if ( !hasAddedRelationships() )
        {
            relationshipsAdded = new RelationshipChangesForNode( DiffStrategy.ADD, state );
        }
        relationshipsAdded.addRelationship( relId, typeId, direction );
    }

    public void removeRelationship( long relId, int typeId, Direction direction )
    {
        if ( hasAddedRelationships() )
        {
            if ( relationshipsAdded.removeRelationship( relId, typeId, direction ) )
            {
                // This was a rel that was added in this tx, no need to add it to the remove list, instead we just
                // remove it from added relationships.
                return;
            }
        }
        if ( !hasRemovedRelationships() )
        {
            relationshipsRemoved = new RelationshipChangesForNode( DiffStrategy.REMOVE, state );
        }
        relationshipsRemoved.addRelationship( relId, typeId, direction );
    }

    @Override
    public void clear()
    {
        super.clear();
        if ( relationshipsAdded != null )
        {
            relationshipsAdded.clear();
        }
        if ( relationshipsRemoved != null )
        {
            relationshipsRemoved.clear();
        }
        if ( labelDiffSets != null )
        {
            labelDiffSets.clear();
        }
        if ( indexDiffs != null )
        {
            indexDiffs.clear();
        }
    }

    @Override
    public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels )
    {
        if ( hasAddedRelationships() )
        {
            return relationshipsAdded.augmentRelationships( direction, rels );
        }
        return rels;
    }

    @Override
    public RelationshipIterator augmentRelationships( Direction direction, int[] types,
            RelationshipIterator rels )
    {
        if ( hasAddedRelationships() )
        {
            return relationshipsAdded.augmentRelationships( direction, types, rels );
        }
        return rels;
    }

    @Override
    public int augmentDegree( Direction direction, int degree )
    {
        if ( hasAddedRelationships() )
        {
            degree = relationshipsAdded.augmentDegree( direction, degree );
        }
        if ( hasRemovedRelationships() )
        {
            degree = relationshipsRemoved.augmentDegree( direction, degree );
        }
        return degree;
    }

    @Override
    public int augmentDegree( Direction direction, int degree, int typeId )
    {
        if ( hasAddedRelationships() )
        {
            degree = relationshipsAdded.augmentDegree( direction, degree, typeId );
        }
        if ( hasRemovedRelationships() )
        {
            degree = relationshipsRemoved.augmentDegree( direction, degree, typeId );
        }
        return degree;
    }

    @Override
    public void accept( NodeState.Visitor visitor ) throws ConstraintValidationKernelException
    {
        super.accept( visitor );
        if ( labelDiffSets != null )
        {
            visitor.visitLabelChanges( getId(), labelDiffSets.getAdded(), labelDiffSets.getRemoved() );
        }
    }

    private boolean hasAddedRelationships()
    {
        return relationshipsAdded != null;
    }

    private boolean hasRemovedRelationships()
    {
        return relationshipsRemoved != null;
    }

    @Override
    public PrimitiveIntIterator relationshipTypes()
    {
        if ( hasAddedRelationships() )
        {
            return relationshipsAdded.relationshipTypes();
        }
        return PrimitiveIntCollections.emptyIterator();
    }

    @Override
    public UpdateTriState labelState( int labelId )
    {
        ReadableDiffSets<Integer> labelDiff = labelDiffSets();
        if ( labelDiff.isAdded( labelId ) )
        {
            return UpdateTriState.ADDED;
        }
        if ( labelDiff.isRemoved( labelId ) )
        {
            return UpdateTriState.REMOVED;
        }
        return UpdateTriState.UNTOUCHED;
    }

    public void addIndexDiff( DiffSets<Long> diff )
    {
        if ( indexDiffs == null )
        {
            indexDiffs = Collections.newSetFromMap( new IdentityHashMap<DiffSets<Long>, Boolean>() );
        }
        indexDiffs.add( diff );
    }

    public void removeIndexDiff( DiffSets<Long> diff )
    {
        if ( indexDiffs != null )
        {
            indexDiffs.remove( diff );
        }
    }

    public void clearIndexDiffs( long nodeId )
    {
        if ( indexDiffs != null )
        {
            for ( DiffSets<Long> diff : indexDiffs )
            {
                if ( diff.getAdded().contains( nodeId ) )
                {
                    diff.remove( nodeId );
                }
                else if ( diff.getRemoved().contains( nodeId ) )
                {
                    diff.add( nodeId );
                }
            }
        }
    }

    @Override
    public boolean hasChanges()
    {
        return super.hasChanges() || hasAddedRelationships() || hasRemovedRelationships();
    }

    @Override
    public PrimitiveLongIterator getAddedRelationships( Direction direction )
    {
        return relationshipsAdded != null ? relationshipsAdded.getRelationships( direction ) :
            PrimitiveLongCollections.emptyIterator();
    }

    @Override
    public PrimitiveLongIterator getAddedRelationships( Direction direction, int[] relTypes )
    {
        return relationshipsAdded != null ? relationshipsAdded.getRelationships( direction, relTypes ) :
            PrimitiveLongCollections.emptyIterator();
    }

    public abstract static class Defaults extends StateDefaults<Long, NodeState, NodeStateImpl>
    {
        @Override
        final NodeStateImpl createValue( Long id, TxState state )
        {
            return new NodeStateImpl( id, state );
        }

        @Override
        final NodeState defaultValue()
        {
            return DEFAULT;
        }

        private static final NodeState DEFAULT = new NodeState()
        {
            @Override
            public Iterator<StorageProperty> addedProperties()
            {
                return Iterators.emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> changedProperties()
            {
                return Iterators.emptyIterator();
            }

            @Override
            public Iterator<Integer> removedProperties()
            {
                return Iterators.emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> addedAndChangedProperties()
            {
                return Iterators.emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
            {
                return iterator;
            }

            @Override
            public void accept( PropertyContainerState.Visitor visitor )
            {
            }

            @Override
            public ReadableDiffSets<Integer> labelDiffSets()
            {
                return ReadableDiffSets.Empty.instance();
            }

            @Override
            public RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels )
            {
                return rels;
            }

            @Override
            public RelationshipIterator augmentRelationships( Direction direction, int[] types,
                    RelationshipIterator rels )
            {
                return rels;
            }

            @Override
            public int augmentDegree( Direction direction, int degree )
            {
                return degree;
            }

            @Override
            public int augmentDegree( Direction direction, int degree, int typeId )
            {
                return degree;
            }

            @Override
            public void accept( NodeState.Visitor visitor )
            {
            }

            @Override
            public PrimitiveIntIterator relationshipTypes()
            {
                return Primitive.intSet().iterator();
            }

            @Override
            public UpdateTriState labelState( int labelId )
            {
                return UpdateTriState.UNTOUCHED;
            }

            @Override
            public long getId()
            {
                throw new UnsupportedOperationException( "id not defined" );
            }

            @Override
            public boolean hasChanges()
            {
                return false;
            }

            @Override
            public StorageProperty getChangedProperty( int propertyKeyId )
            {
                return null;
            }

            @Override
            public StorageProperty getAddedProperty( int propertyKeyId )
            {
                return null;
            }

            @Override
            public boolean isPropertyRemoved( int propertyKeyId )
            {
                return false;
            }

            @Override
            public PrimitiveLongIterator getAddedRelationships( Direction direction )
            {
                return null;
            }

            @Override
            public PrimitiveLongIterator getAddedRelationships( Direction direction, int[] relTypes )
            {
                return null;
            }
        };
    }
}
