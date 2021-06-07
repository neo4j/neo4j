/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.newapi;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.ScanQuery;
import org.neo4j.kernel.impl.newapi.TokenIndexPartitionedScanTestSuite.TokenScanQuery;
import org.neo4j.util.Id;

class PartitionedScanFactories
{
    abstract static class PartitionedScanFactory<SCAN_QUERY extends ScanQuery<?>, CURSOR extends Cursor>
    {
        abstract PartitionedScan<CURSOR> partitionedScan( KernelTransaction tx, SCAN_QUERY query, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException;

        abstract CURSOR getCursor( KernelTransaction tx );

        abstract long getEntityReference( CURSOR cursor );

        final String name()
        {
            return getClass().getSimpleName();
        }
    }

    abstract static class TokenIndex<CURSOR extends Cursor>
            extends PartitionedScanFactory<TokenScanQuery,CURSOR>
    {
        protected final TokenReadSession getSession( KernelTransaction tx, TokenScanQuery query )
                throws IndexNotFoundKernelException
        {
            final var index = tx.schemaRead().indexGetForName( query.indexName() );
            return tx.dataRead().tokenReadSession( index );
        }
    }

    static final class NodeLabelIndex extends TokenIndex<NodeLabelIndexCursor>
    {
        public static final NodeLabelIndex FACTORY = new NodeLabelIndex();

        private NodeLabelIndex()
        {
        }

        @Override
        PartitionedScan<NodeLabelIndexCursor> partitionedScan( KernelTransaction tx, TokenScanQuery scanQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().nodeLabelScan( getSession( tx, scanQuery ), scanQuery.get(), desiredNumberOfPartitions );
        }

        @Override
        NodeLabelIndexCursor getCursor( KernelTransaction tx )
        {
            return tx.cursors().allocateNodeLabelIndexCursor( tx.cursorContext() );
        }

        @Override
        long getEntityReference( NodeLabelIndexCursor cursor )
        {
            return cursor.nodeReference();
        }
    }

    static final class RelationshipTypeIndex extends TokenIndex<RelationshipTypeIndexCursor>
    {
        public static final RelationshipTypeIndex FACTORY = new RelationshipTypeIndex();

        private RelationshipTypeIndex()
        {
        }

        @Override
        PartitionedScan<RelationshipTypeIndexCursor> partitionedScan( KernelTransaction tx, TokenScanQuery scanQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().relationshipTypeScan( getSession( tx, scanQuery ), scanQuery.get(), desiredNumberOfPartitions );
        }

        @Override
        RelationshipTypeIndexCursor getCursor( KernelTransaction tx )
        {
            return tx.cursors().allocateRelationshipTypeIndexCursor( tx.cursorContext() );
        }

        @Override
        long getEntityReference( RelationshipTypeIndexCursor cursor )
        {
            return cursor.relationshipReference();
        }
    }

    abstract static class Tag<TAG> implements Supplier<TAG>
    {
        protected abstract TagFromName<TAG> fromName();

        abstract int createId( KernelTransaction tx, TAG TAG ) throws KernelException;

        @Override
        public final TAG get()
        {
            final var id = new Id( UUID.randomUUID() );
            return fromName().generate( name() + '_' + id );
        }

        final String name()
        {
            return getClass().getSimpleName();
        }

        final List<TAG> generate( int numberOfTags )
        {
            return Stream.generate( this ).limit( numberOfTags ).collect( Collectors.toUnmodifiableList() );
        }

        final List<Integer> createIds( KernelTransaction tx, Iterable<TAG> tags ) throws KernelException
        {
            final var ids = new ArrayList<Integer>();
            for ( var tag : tags )
            {
                ids.add( createId( tx, tag ) );
            }
            return ids;
        }

        final List<Integer> generateAndCreateIds( KernelTransaction tx, int numberOfTags ) throws KernelException
        {
            return createIds( tx, generate( numberOfTags ) );
        }

        interface TagFromName<TAG>
        {
            TAG generate( String name );
        }
    }

    static final class Label extends Tag<org.neo4j.graphdb.Label>
    {
        public static final Label FACTORY = new Label();

        private Label()
        {
        }

        @Override
        protected TagFromName<org.neo4j.graphdb.Label> fromName()
        {
            return org.neo4j.graphdb.Label::label;
        }

        @Override
        int createId( KernelTransaction tx, org.neo4j.graphdb.Label label ) throws KernelException
        {
            return tx.tokenWrite().labelGetOrCreateForName( label.name() );
        }
    }

    static final class RelationshipType extends Tag<org.neo4j.graphdb.RelationshipType>
    {
        public static final RelationshipType FACTORY = new RelationshipType();

        private RelationshipType()
        {
        }

        @Override
        protected TagFromName<org.neo4j.graphdb.RelationshipType> fromName()
        {
            return org.neo4j.graphdb.RelationshipType::withName;
        }

        @Override
        int createId( KernelTransaction tx, org.neo4j.graphdb.RelationshipType relType ) throws KernelException
        {
            return tx.tokenWrite().relationshipTypeGetOrCreateForName( relType.name() );
        }
    }
}
