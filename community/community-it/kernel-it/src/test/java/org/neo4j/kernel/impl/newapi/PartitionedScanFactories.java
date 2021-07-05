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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.kernel.impl.newapi.PropertyIndexScanPartitionedScanTestSuite.PropertyKeyScanQuery;
import org.neo4j.kernel.impl.newapi.PropertyIndexSeekPartitionedScanTestSuite.PropertyKeySeekQuery;
import org.neo4j.kernel.impl.newapi.TokenIndexScanPartitionedScanTestSuite.TokenScanQuery;
import org.neo4j.util.Id;

class PartitionedScanFactories
{
    abstract static class PartitionedScanFactory<QUERY extends Query<?>, CURSOR extends Cursor>
    {
        abstract PartitionedScan<CURSOR> partitionedScan( KernelTransaction tx, QUERY query, int desiredNumberOfPartitions )
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

    static final class NodeLabelIndexScan extends TokenIndex<NodeLabelIndexCursor>
    {
        public static final NodeLabelIndexScan FACTORY = new NodeLabelIndexScan();

        private NodeLabelIndexScan()
        {
        }

        @Override
        PartitionedScan<NodeLabelIndexCursor> partitionedScan( KernelTransaction tx, TokenScanQuery tokenScanQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().nodeLabelScan( getSession( tx, tokenScanQuery ), desiredNumberOfPartitions, CursorContext.NULL, tokenScanQuery.get() );
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

    static final class RelationshipTypeIndexScan extends TokenIndex<RelationshipTypeIndexCursor>
    {
        public static final RelationshipTypeIndexScan FACTORY = new RelationshipTypeIndexScan();

        private RelationshipTypeIndexScan()
        {
        }

        @Override
        PartitionedScan<RelationshipTypeIndexCursor> partitionedScan( KernelTransaction tx, TokenScanQuery tokenScanQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().relationshipTypeScan( getSession( tx, tokenScanQuery ), desiredNumberOfPartitions, CursorContext.NULL, tokenScanQuery.get() );
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

    abstract static class PropertyIndex<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanFactory<QUERY,CURSOR>
    {
        abstract SchemaDescriptor getSchemaDescriptor( int tokenId, int... propKeyIds );

        abstract Tag<?> getTokenFactory();

        final PropertyKey getPropKeyFactory()
        {
            return PropertyKey.FACTORY;
        }

        final String getIndexName( int tokenId, int... propKeyIds ) {
            return String.format( "%s[%s[%d] {%s}]", name(), getTokenFactory().name(), tokenId,
                                  Arrays.stream( propKeyIds ).mapToObj( String::valueOf ).collect( Collectors.joining( "," ) ) );
        }

        protected final IndexReadSession getSession( KernelTransaction tx, QUERY query )
                throws IndexNotFoundKernelException
        {
            final var index = tx.schemaRead().indexGetForName( query.indexName() );
            return tx.dataRead().indexReadSession( index );
        }
    }

    static final class NodePropertyIndexSeek extends PropertyIndex<PropertyKeySeekQuery,NodeValueIndexCursor>
    {
        public static final NodePropertyIndexSeek FACTORY = new NodePropertyIndexSeek();

        private NodePropertyIndexSeek()
        {
        }

        @Override
        PartitionedScan<NodeValueIndexCursor> partitionedScan( KernelTransaction tx, PropertyKeySeekQuery propertyKeySeekQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().nodeIndexSeek( getSession( tx, propertyKeySeekQuery ), desiredNumberOfPartitions,
                                                QueryContext.NULL_CONTEXT, propertyKeySeekQuery.get() );
        }

        @Override
        NodeValueIndexCursor getCursor( KernelTransaction tx )
        {
            return tx.cursors().allocateNodeValueIndexCursor( tx.cursorContext(), tx.memoryTracker() );
        }

        @Override
        long getEntityReference( NodeValueIndexCursor cursor )
        {
            return cursor.nodeReference();
        }

        @Override
        Label getTokenFactory()
        {
            return Label.FACTORY;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor( int labelId, int... propKeyIds )
        {
            return SchemaDescriptors.forLabel( labelId, propKeyIds );
        }
    }

    static final class NodePropertyIndexScan extends PropertyIndex<PropertyKeyScanQuery,NodeValueIndexCursor>
    {
        public static final NodePropertyIndexScan FACTORY = new NodePropertyIndexScan();

        private NodePropertyIndexScan()
        {
        }

        @Override
        PartitionedScan<NodeValueIndexCursor> partitionedScan( KernelTransaction tx, PropertyKeyScanQuery propertyKeyScanQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().nodeIndexScan( getSession( tx, propertyKeyScanQuery ), desiredNumberOfPartitions, QueryContext.NULL_CONTEXT );
        }

        @Override
        NodeValueIndexCursor getCursor( KernelTransaction tx )
        {
            return tx.cursors().allocateNodeValueIndexCursor( tx.cursorContext(), tx.memoryTracker() );
        }

        @Override
        long getEntityReference( NodeValueIndexCursor cursor )
        {
            return cursor.nodeReference();
        }

        @Override
        Label getTokenFactory()
        {
            return Label.FACTORY;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor( int labelId, int... propKeyIds )
        {
            return SchemaDescriptors.forLabel( labelId, propKeyIds );
        }
    }

    static final class RelationshipPropertyIndexSeek extends PropertyIndex<PropertyKeySeekQuery,RelationshipValueIndexCursor>
    {
        public static final RelationshipPropertyIndexSeek FACTORY = new RelationshipPropertyIndexSeek();

        private RelationshipPropertyIndexSeek()
        {
        }

        @Override
        PartitionedScan<RelationshipValueIndexCursor> partitionedScan( KernelTransaction tx, PropertyKeySeekQuery propertyKeySeekQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().relationshipIndexSeek( getSession( tx, propertyKeySeekQuery ), desiredNumberOfPartitions,
                                                        QueryContext.NULL_CONTEXT, propertyKeySeekQuery.get());
        }

        @Override
        RelationshipValueIndexCursor getCursor( KernelTransaction tx )
        {
            return tx.cursors().allocateRelationshipValueIndexCursor( tx.cursorContext(), tx.memoryTracker() );
        }

        @Override
        long getEntityReference( RelationshipValueIndexCursor cursor )
        {
            return cursor.relationshipReference();
        }

        @Override
        RelationshipType getTokenFactory()
        {
            return RelationshipType.FACTORY;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor( int relTypeId, int... propKeyIds )
        {
            return SchemaDescriptors.forRelType( relTypeId, propKeyIds );
        }
    }

    static final class RelationshipPropertyIndexScan extends PropertyIndex<PropertyKeyScanQuery,RelationshipValueIndexCursor>
    {
        public static final RelationshipPropertyIndexScan FACTORY = new RelationshipPropertyIndexScan();

        private RelationshipPropertyIndexScan()
        {
        }

        @Override
        PartitionedScan<RelationshipValueIndexCursor> partitionedScan( KernelTransaction tx, PropertyKeyScanQuery propertyKeyScanQuery, int desiredNumberOfPartitions )
                throws IndexNotFoundKernelException, IndexNotApplicableKernelException
        {
            return tx.dataRead().relationshipIndexScan( getSession( tx, propertyKeyScanQuery ), desiredNumberOfPartitions, QueryContext.NULL_CONTEXT );
        }

        @Override
        RelationshipValueIndexCursor getCursor( KernelTransaction tx )
        {
            return tx.cursors().allocateRelationshipValueIndexCursor( tx.cursorContext(), tx.memoryTracker() );
        }

        @Override
        long getEntityReference( RelationshipValueIndexCursor cursor )
        {
            return cursor.relationshipReference();
        }

        @Override
        RelationshipType getTokenFactory()
        {
            return RelationshipType.FACTORY;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor( int relTypeId, int... propKeyIds )
        {
            return SchemaDescriptors.forRelType( relTypeId, propKeyIds );
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
            for ( final var tag : tags )
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

    static final class PropertyKey extends Tag<String>
    {
        public static final PropertyKey FACTORY = new PropertyKey();

        private PropertyKey()
        {
        }

        @Override
        protected TagFromName<String> fromName()
        {
            return name -> name;
        }

        @Override
        int createId( KernelTransaction tx, String propertyKey ) throws KernelException
        {
            return tx.tokenWrite().propertyKeyGetOrCreateForName( propertyKey );
        }
    }
}
