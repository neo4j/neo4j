/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cursor.Cursor;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.txstate.UpdateTriState;
import org.neo4j.kernel.impl.api.cursor.TxAllPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxIteratorRelationshipCursor;
import org.neo4j.kernel.impl.api.cursor.TxLabelCursor;
import org.neo4j.kernel.impl.api.cursor.TxSingleLabelCursor;
import org.neo4j.kernel.impl.api.cursor.TxSinglePropertyCursor;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode.DiffStrategy;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

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
        void visitLabelChanges( long nodeId, Set<Integer> added, Set<Integer> removed )
                throws ConstraintValidationKernelException;

        void visitRelationshipChanges(
                long nodeId, RelationshipChangesForNode added, RelationshipChangesForNode removed );
    }

    ReadableDiffSets<Integer> labelDiffSets();

    RelationshipIterator augmentRelationships( Direction direction, RelationshipIterator rels );

    RelationshipIterator augmentRelationships( Direction direction, int[] types, RelationshipIterator rels );

    PrimitiveLongIterator addedRelationships( Direction direction, int[] types );

    Cursor<LabelItem> augmentLabelCursor( InstanceCache<TxLabelCursor> labelCursorCache, Cursor<LabelItem> cursor );

    Cursor<LabelItem> augmentLabelCursor( InstanceCache<TxSingleLabelCursor> labelCursorCache,
            Cursor<LabelItem> cursor,
            int labelId );

    Cursor<RelationshipItem> augmentNodeRelationshipCursor( InstanceCache<TxIteratorRelationshipCursor>
            nodeRelationshipCursor,
            Cursor<RelationshipItem> cursor,
            Direction direction, int[] relTypes );

    int augmentDegree( Direction direction, int degree );

    int augmentDegree( Direction direction, int degree, int typeId );

    void accept( NodeState.Visitor visitor ) throws ConstraintValidationKernelException;

    PrimitiveIntIterator relationshipTypes();

    UpdateTriState labelState( int labelId );

    long getId();

    class Mutable extends PropertyContainerState.Mutable implements NodeState
    {
        private DiffSets<Integer> labelDiffSets;
        private RelationshipChangesForNode relationshipsAdded;
        private RelationshipChangesForNode relationshipsRemoved;
        private Set<DiffSets<Long>> indexDiffs;
        private final TxState state;

        private Mutable( long id, TxState state )
        {
            super( id, EntityType.NODE );
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
        public RelationshipIterator addedRelationships( Direction direction, int[] types )
        {
            if ( hasAddedRelationships() )
            {
                return relationshipsAdded.augmentRelationships( direction, types, RelationshipIterator.EMPTY );
            }
            return null;
        }

        @Override
        public Cursor<LabelItem> augmentLabelCursor( InstanceCache<TxLabelCursor> labelCursorCache,
                Cursor<LabelItem> cursor )
        {
            if ( labelDiffSets == null )
            {
                return cursor;
            }
            else
            {
                return labelCursorCache.get().init( cursor, labelDiffSets );
            }
        }

        @Override
        public Cursor<LabelItem> augmentLabelCursor( InstanceCache<TxSingleLabelCursor> labelCursorCache,
                Cursor<LabelItem> cursor,
                int labelId )
        {
            if ( labelDiffSets == null )
            {
                return cursor;
            }
            else
            {
                return labelCursorCache.get().init( cursor, labelDiffSets );
            }
        }

        @Override
        public Cursor<RelationshipItem> augmentNodeRelationshipCursor( InstanceCache<TxIteratorRelationshipCursor>
                nodeRelationshipCursorCache,
                Cursor<RelationshipItem> cursor,
                Direction direction,
                int[] relTypes )
        {
            if ( hasAddedRelationships() || hasRemovedRelationships() )
            {
                if ( relTypes == null )
                {
                    return nodeRelationshipCursorCache.get().init( cursor,
                            relationshipsAdded != null ?
                                    relationshipsAdded.augmentRelationships( direction, RelationshipIterator.EMPTY ) :
                                    RelationshipIterator.EMPTY );
                }
                else
                {
                    return nodeRelationshipCursorCache.get().init( cursor,
                            relationshipsAdded != null ?
                                    relationshipsAdded.augmentRelationships( direction, relTypes,
                                            RelationshipIterator.EMPTY ) :
                                    RelationshipIterator.EMPTY );
                }

            }
            else
            {
                return cursor;
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
        public void accept( NodeState.Visitor visitor ) throws ConstraintValidationKernelException
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
        final Mutable createValue( Long id, TxState state )
        {
            return new Mutable( id, state );
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
            public PrimitiveLongIterator addedRelationships( Direction direction, int[] types )
            {
                return Primitive.iterator();
            }

            @Override
            public Cursor<LabelItem> augmentLabelCursor( InstanceCache<TxLabelCursor> labelCursorCache,
                    Cursor<LabelItem> cursor )
            {
                return cursor;
            }

            @Override
            public Cursor<LabelItem> augmentLabelCursor( InstanceCache<TxSingleLabelCursor> labelCursorCache,
                    Cursor<LabelItem> cursor, int labelId )
            {
                return cursor;
            }

            @Override
            public Cursor<RelationshipItem> augmentNodeRelationshipCursor(
                    InstanceCache<TxIteratorRelationshipCursor> nodeRelationshipCursor,
                    Cursor<RelationshipItem> cursor, Direction direction, int[] relTypes )
            {
                return cursor;
            }

            @Override
            public Cursor<PropertyItem> augmentPropertyCursor( Supplier<TxAllPropertyCursor> propertyCursor,
                    Cursor<PropertyItem> cursor )
            {
                return cursor;
            }

            @Override
            public Cursor<PropertyItem> augmentSinglePropertyCursor( Supplier<TxSinglePropertyCursor> propertyCursor,
                    Cursor<PropertyItem> cursor, int propertyKeyId )
            {
                return cursor;
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
