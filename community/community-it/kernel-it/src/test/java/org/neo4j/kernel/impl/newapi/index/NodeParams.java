/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

package org.neo4j.kernel.impl.newapi.index;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.stringValue;

public class NodeParams implements EntityParams<NodeValueIndexCursor>
{
    @Override
    public long entityWithProp( Transaction tx, String token, String key, Object value )
    {
        Node node = tx.createNode( label( token ) );
        node.setProperty( key, value );
        return node.getId();
    }

    @Override
    public long entityNoTokenWithProp( Transaction tx, String key, Object value )
    {
        Node node = tx.createNode();
        node.setProperty( key, value );
        return node.getId();
    }

    @Override
    public long entityWithTwoProps( Transaction tx, String token, String key1, String value1, String key2, String value2 )
    {
        Node node = tx.createNode( label( token ) );
        node.setProperty( key1, value1 );
        node.setProperty( key2, value2 );
        return node.getId();
    }

    @Override
    public boolean tokenlessEntitySupported()
    {
        return true;
    }

    @Override
    public NodeValueIndexCursor allocateEntityValueIndexCursor( KernelTransaction tx, CursorFactory cursorFactory )
    {
        return cursorFactory.allocateNodeValueIndexCursor( NULL, tx.memoryTracker() );
    }

    @Override
    public long entityReference( NodeValueIndexCursor cursor )
    {
        return cursor.nodeReference();
    }

    @Override
    public void entityIndexSeek( KernelTransaction tx, IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints,
                                 IndexQuery... query ) throws KernelException
    {
        tx.dataRead().nodeIndexSeek( index, cursor, constraints, query );
    }

    @Override
    public void entityIndexScan( KernelTransaction tx, IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints )
            throws KernelException
    {
        tx.dataRead().nodeIndexScan( index, cursor, constraints );
    }

    @Override
    public void createEntityIndex( Transaction tx, String entityToken, String propertyKey, String indexName )
    {
        tx.schema().indexFor( label( entityToken ) ).on( propertyKey ).withName( indexName ).create();
    }

    @Override
    public void createCompositeEntityIndex( Transaction tx, String entityToken, String propertyKey1, String propertyKey2, String indexName )
    {
        tx.schema().indexFor( label( entityToken ) ).on( propertyKey1 ).on( propertyKey2 ).withName( indexName ).create();
    }

    @Override
    public void entitySetProperty( KernelTransaction tx, long entityId, int propId, String value ) throws KernelException
    {
        tx.dataWrite().nodeSetProperty( entityId, propId, stringValue( value ) );
    }

    @Override
    public int entityTokenId( KernelTransaction tx, String tokenName )
    {
        return tx.token().nodeLabel( tokenName );
    }

    @Override
    public SchemaDescriptor schemaDescriptor( int tokenId, int propId )
    {
        return SchemaDescriptor.forLabel( tokenId, propId );
    }

    @Override
    public Value getPropertyValueFromStore( KernelTransaction tx, CursorFactory cursorFactory, long reference )
    {
        try ( NodeCursor storeCursor = cursorFactory.allocateNodeCursor( NULL );
              PropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            tx.dataRead().singleNode( reference, storeCursor );
            storeCursor.next();
            storeCursor.properties( propertyCursor );
            propertyCursor.next();
            return propertyCursor.propertyValue();
        }
    }

    @Override
    public void entityDelete( KernelTransaction tx, long reference ) throws InvalidTransactionTypeKernelException
    {
        tx.dataWrite().nodeDelete( reference );
    }

    @Override
    public void entityRemoveToken( KernelTransaction tx, long entityId, int tokenId ) throws KernelException
    {
        tx.dataWrite().nodeRemoveLabel( entityId, tokenId );
    }

    @Override
    public void entityAddToken( KernelTransaction tx, long entityId, int tokenId ) throws KernelException
    {
        tx.dataWrite().nodeAddLabel( entityId, tokenId );
    }
}
