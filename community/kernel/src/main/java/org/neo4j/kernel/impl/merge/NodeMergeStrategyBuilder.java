/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.PrimitiveLongPredicate;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.PrimitiveLongIteratorForArray;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;

import static org.neo4j.helpers.collection.IteratorUtil.filter;
import static org.neo4j.helpers.collection.IteratorUtil.filterSingle;
import static org.neo4j.helpers.collection.IteratorUtil.singletonPrimitiveLongIterator;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

class NodeMergeStrategyBuilder
{
    private final ArrayList<NodeMergeStrategy> strategies = new ArrayList<>();

    NodeMergeStrategyBuilder addUsingUniqueIndex( int labelId, DefinedProperty property, IndexDescriptor index )
    {
        strategies.add( new UniqueNodeMergeStrategy( index, labelId, property ) );
        return this;
    }

    NodeMergeStrategyBuilder addUsingIndex( int labelId, DefinedProperty property, IndexDescriptor index )
    {
        strategies.add( new IndexNodeMergeStrategy( index, labelId, property ) );
        return this;
    }

    NodeMergeStrategyBuilder addUsingLabel( int labelId, DefinedProperty property )
    {
        strategies.add( new LabelScanNodeMergeStrategy( labelId, property ) );
        return this;
    }

    List<NodeMergeStrategy> buildStrategies()
    {
        // sort the permutations to get predictable lock ordering
        sort( strategies );

        // return stable view
        return unmodifiableList( strategies );
    }

    private static final class UniqueNodeMergeStrategy extends NodeMergeStrategy
    {
        private final IndexDescriptor index;

        UniqueNodeMergeStrategy( IndexDescriptor index, int labelId, DefinedProperty property )
        {
            super( Type.UNIQUE_INDEX, labelId, property );
            this.index = index;
        }

        @Override
        protected long lookupSingle( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
                throws IndexBrokenKernelException
        {
            try
            {
                long candidateId = ops.nodeGetUniqueFromIndexLookup( index, property.value() );
                return nodePredicate.accept( candidateId ) ? candidateId : NO_SUCH_NODE;
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new ThisShouldNotHappenError( "Tobias", "Uniqueness Constraint should have index.", e );
            }
        }

        @Override
        protected PrimitiveLongIterator lookupAll( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
                throws IndexBrokenKernelException

        {
            long item = lookupSingle( ops, nodePredicate );
            return NO_SUCH_NODE == item ? PrimitiveLongIteratorForArray.EMPTY : singletonPrimitiveLongIterator( item );
        }
    }

    private static class IndexNodeMergeStrategy extends NodeMergeStrategy
    {
        private final IndexDescriptor index;

        IndexNodeMergeStrategy( IndexDescriptor index, int labelId, DefinedProperty property )
        {
            super( Type.REGULAR_INDEX, labelId, property );
            this.index = index;
        }

        @Override
        protected long lookupSingle( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
                throws IndexBrokenKernelException
        {
            try
            {
                PrimitiveLongIterator iterator = ops.nodesGetFromIndexLookup( index, property.value() );
                return filterSingle( nodePredicate, iterator, NO_SUCH_NODE );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new ThisShouldNotHappenError( "Stefan", "Index should exist.", e );
            }
            catch ( NoSuchElementException e )
            {
                throw newNotFoundDueToConflictsException( e );
            }
        }

        @Override
        protected PrimitiveLongIterator lookupAll( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
                throws IndexBrokenKernelException

        {
            try
            {
                PrimitiveLongIterator iterator = ops.nodesGetFromIndexLookup( index, property.value() );
                return filter( nodePredicate, iterator );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new ThisShouldNotHappenError( "Stefan", "Index should exist.", e );
            }
        }

    }

    private static final class LabelScanNodeMergeStrategy extends NodeMergeStrategy
    {
        LabelScanNodeMergeStrategy( int labelId, DefinedProperty property )
        {
            super( Type.LABEL_SCAN, labelId, property );
        }

        @Override
        protected long lookupSingle( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
                throws IndexBrokenKernelException
        {
            PrimitiveLongIterator iterator = ops.nodesGetForLabel( labelId );
            try
            {
                return filterSingle( nodePredicate, iterator, NO_SUCH_NODE );
            }
            catch ( NoSuchElementException e )
            {
                throw newNotFoundDueToConflictsException( e );
            }
        }

        @Override
        protected PrimitiveLongIterator lookupAll( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
                throws IndexBrokenKernelException
        {
            PrimitiveLongIterator iterator = ops.nodesGetForLabel( labelId );
            try
            {
                return filter( nodePredicate, iterator );
            }
            catch ( NoSuchElementException e )
            {
                throw newNotFoundDueToConflictsException( e );
            }
        }
    }

    private static NotFoundException newNotFoundDueToConflictsException( NoSuchElementException e )
    {
        return new NotFoundException( "Conflicting nodes found", e );
    }
}
