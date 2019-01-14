/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.api.impl.fulltext.LuceneFulltextDocumentStructure.documentRepresentingProperties;

/**
 * A {@link TxStateVisitor} that adds all entities to a {@link TransactionStateLuceneIndexWriter}, that matches the index according to the
 * {@link FulltextIndexDescriptor}.
 */
class FulltextIndexTransactionStateVisitor extends TxStateVisitor.Adapter
{
    private final FulltextIndexDescriptor descriptor;
    private final SchemaDescriptor schema;
    private final boolean visitingNodes;
    private final int[] entityTokenIds;
    private final Value[] propertyValues;
    private final IntIntHashMap propKeyToIndex;
    private final MutableLongSet modifiedEntityIdsInThisTransaction;
    private final TransactionStateLuceneIndexWriter writer;
    private AllStoreHolder read;
    private NodeCursor nodeCursor;
    private PropertyCursor propertyCursor;
    private RelationshipScanCursor relationshipCursor;

    FulltextIndexTransactionStateVisitor( FulltextIndexDescriptor descriptor, MutableLongSet modifiedEntityIdsInThisTransaction,
            TransactionStateLuceneIndexWriter writer )
    {
        this.descriptor = descriptor;
        this.schema = descriptor.schema();
        this.modifiedEntityIdsInThisTransaction = modifiedEntityIdsInThisTransaction;
        this.writer = writer;
        this.visitingNodes = schema.entityType() == EntityType.NODE;
        entityTokenIds = schema.getEntityTokenIds();
        int[] propertyIds = schema.getPropertyIds();
        propertyValues = new Value[propertyIds.length];
        propKeyToIndex = new IntIntHashMap();
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            propKeyToIndex.put( propertyIds[i], i );
        }
    }

    FulltextIndexTransactionStateVisitor init( AllStoreHolder read, NodeCursor nodeCursor, RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor )
    {
        this.read = read;
        this.nodeCursor = nodeCursor;
        this.relationshipCursor = relationshipCursor;
        this.propertyCursor = propertyCursor;
        return this;
    }

    @Override
    public void visitCreatedNode( long id )
    {
        indexNode( id );
    }

    @Override
    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
    {
        indexRelationship( id );
    }

    @Override
    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
    {
        indexNode( id );
    }

    @Override
    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
    {
        indexRelationship( id );
    }

    @Override
    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
    {
        indexNode( id );
        if ( visitingNodes )
        {
            // Nodes that have had their indexed labels removed will not have their properties indexed, so 'indexNode' would skip them.
            // However, we still need to make sure that they are not included in the result from the base index reader.
            for ( int entityTokenId : entityTokenIds )
            {
                if ( removed.contains( entityTokenId ) )
                {
                    modifiedEntityIdsInThisTransaction.add( id );
                    break;
                }
            }
        }
    }

    private void indexNode( long id )
    {
        if ( visitingNodes )
        {
            read.singleNode( id, nodeCursor );
            if ( nodeCursor.next() )
            {
                LabelSet labels = nodeCursor.labels();
                if ( schema.isAffected( labels.all() ) )
                {
                    nodeCursor.properties( propertyCursor );
                    indexProperties( id );
                }
            }
        }
    }

    private void indexRelationship( long id )
    {
        if ( !visitingNodes )
        {
            read.singleRelationship( id, relationshipCursor );
            if ( relationshipCursor.next() && schema.isAffected( new long[]{relationshipCursor.type()} ) )
            {
                relationshipCursor.properties( propertyCursor );
                indexProperties( id );
            }
        }
    }

    private void indexProperties( long id )
    {
        while ( propertyCursor.next() )
        {
            int propertyKey = propertyCursor.propertyKey();
            int index = propKeyToIndex.getIfAbsent( propertyKey, -1 );
            if ( index != -1 )
            {
                propertyValues[index] = propertyCursor.propertyValue();
            }
        }
        if ( modifiedEntityIdsInThisTransaction.add( id ) )
        {
            try
            {
                writer.addDocument( documentRepresentingProperties( id, descriptor.propertyNames(), propertyValues ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
        Arrays.fill( propertyValues, null );
    }
}
