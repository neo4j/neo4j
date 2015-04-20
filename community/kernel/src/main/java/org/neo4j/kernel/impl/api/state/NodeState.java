/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.UpdateTriState;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;

/**
 * Represents the transactional changes to a node:
 * <ul>
 * <li>{@linkplain #labelDiffSets() Labels} that have been {@linkplain ReadableDiffSets#getAdded() added}
 * or {@linkplain ReadableDiffSets#getRemoved() removed}.</li>
 * <li>Added and removed relationships.</li>
 * <li>{@linkplain PropertyContainerState Changes to properties}.</li>
 * </ul>
 */
public interface NodeState extends PropertyContainerState
{
    interface Visitor extends PropertyContainerState.Visitor
    {
        void visitLabelChanges( long nodeId, Set<Integer> added, Set<Integer> removed );

        void visitRelationshipChanges(
                long nodeId, RelationshipChangesForNode added, RelationshipChangesForNode removed );
    }

    ReadableDiffSets<Integer> labelDiffSets();

    PrimitiveLongIterator augmentRelationships( Direction direction, PrimitiveLongIterator rels );

    PrimitiveLongIterator augmentRelationships( Direction direction, int[] types, PrimitiveLongIterator rels );

    PrimitiveLongIterator addedRelationships( Direction direction, int[] types );

    int augmentDegree( Direction direction, int degree );

    int augmentDegree( Direction direction, int degree, int typeId );

    void accept( NodeState.Visitor visitor );

    PrimitiveIntIterator relationshipTypes();

    UpdateTriState labelState( int labelId );

    long getId();

    class Mutable extends PropertyContainerState.Mutable implements NodeState
    {
        private DiffSets<Integer> labelDiffSets;
        private RelationshipChangesForNode relationshipsAdded;
        private RelationshipChangesForNode relationshipsRemoved;
        private Set<DiffSets<Long>> indexDiffs;

        private Mutable( long id )
        {
            super( id );
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
                relationshipsAdded = new RelationshipChangesForNode( DiffStrategy.ADD );
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
                relationshipsRemoved = new RelationshipChangesForNode( DiffStrategy.REMOVE );
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
        public PrimitiveLongIterator augmentRelationships( Direction direction, PrimitiveLongIterator rels )
        {
            if ( hasAddedRelationships() )
            {
                return relationshipsAdded.augmentRelationships( direction, rels );
            }
            return rels;
        }

        @Override
        public PrimitiveLongIterator augmentRelationships( Direction direction, int[] types,
                                                           PrimitiveLongIterator rels )
        {
            if ( hasAddedRelationships() )
            {
                return relationshipsAdded.augmentRelationships( direction, types, rels );
            }
            return rels;
        }

        @Override
        public PrimitiveLongIterator addedRelationships( Direction direction, int[] types )
        {
            if ( hasAddedRelationships() )
            {
                return relationshipsAdded.augmentRelationships( direction, types, emptyIterator() );
            }
            return null;
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
        public void accept( NodeState.Visitor visitor )
        {
            super.accept( visitor );
            if ( labelDiffSets != null )
            {
                visitor.visitLabelChanges( getId(), labelDiffSets.getAdded(), labelDiffSets.getRemoved() );
            }
            if ( relationshipsAdded != null || relationshipsRemoved != null )
            {
                visitor.visitRelationshipChanges( getId(), relationshipsAdded, relationshipsRemoved );
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
    }

    abstract class Defaults extends StateDefaults<Long, NodeState, NodeState.Mutable>
    {
        @Override
        final Mutable createValue( Long id )
        {
            return new Mutable( id );
        }

        @Override
        final NodeState defaultValue()
        {
            return DEFAULT;
        }

        private static final NodeState DEFAULT = new NodeState()
        {
            @Override
            public Iterator<DefinedProperty> addedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<DefinedProperty> changedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<Integer> removedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<DefinedProperty> addedAndChangedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<DefinedProperty> augmentProperties( Iterator<DefinedProperty> iterator )
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
            public PrimitiveLongIterator augmentRelationships( Direction direction, PrimitiveLongIterator rels )
            {
                return rels;
            }

            @Override
            public PrimitiveLongIterator augmentRelationships( Direction direction, int[] types,
                                                               PrimitiveLongIterator rels )
            {
                return rels;
            }

            @Override
            public PrimitiveLongIterator addedRelationships( Direction direction, int[] types )
            {
                return Primitive.iterator();
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
        };
    }
}
