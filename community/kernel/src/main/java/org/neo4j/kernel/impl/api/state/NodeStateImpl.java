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
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.PropertyContainerState;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

import static java.util.Collections.emptyIterator;
import static org.neo4j.collection.primitive.Primitive.intSet;

public class NodeStateImpl extends PropertyContainerStateImpl implements NodeState
{
    private DiffSets<Integer> labelDiffSets;
    private RelationshipChangesForNode relationshipsAdded;
    private RelationshipChangesForNode relationshipsRemoved;
    private Set<DiffSets<Long>> indexDiffs; // TODO: does this really fill any function?
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

    DiffSets<Integer> getOrCreateLabelDiffSets()
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
    public void accept( NodeState.Visitor visitor ) throws ConstraintValidationException
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
    public PrimitiveIntSet relationshipTypes()
    {
        if ( hasAddedRelationships() )
        {
            return relationshipsAdded.relationshipTypes();
        }
        return intSet();
    }

    void addIndexDiff( DiffSets<Long> diff )
    {
        if ( indexDiffs == null )
        {
            indexDiffs = Collections.newSetFromMap( new IdentityHashMap<DiffSets<Long>, Boolean>() );
        }
        indexDiffs.add( diff );
    }

    void removeIndexDiff( DiffSets<Long> diff )
    {
        if ( indexDiffs != null )
        {
            indexDiffs.remove( diff );
        }
    }

    void clearIndexDiffs( long nodeId )
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
                return emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> changedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<Integer> removedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> addedAndChangedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
            {
                return iterator;
            }

            @Override
            public void accept( PropertyContainerState.Visitor visitor ) throws ConstraintValidationException
            {
            }

            @Override
            public ReadableDiffSets<Integer> labelDiffSets()
            {
                return ReadableDiffSets.Empty.instance();
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
            public PrimitiveIntSet relationshipTypes()
            {
                return Primitive.intSet();
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
