/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi.index;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.ValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;

public interface EntityParams<CURSOR extends Cursor & ValueIndexCursor> {
    long entityWithProp(Transaction tx, String token, String key, Object value);

    long entityNoTokenWithProp(Transaction tx, String key, Object value);

    long entityWithTwoProps(Transaction tx, String token, String key1, String value1, String key2, String value2);

    boolean tokenlessEntitySupported();

    CURSOR allocateEntityValueIndexCursor(KernelTransaction tx, CursorFactory cursorFactory);

    long entityReference(CURSOR cursor);

    void entityIndexSeek(
            KernelTransaction tx,
            IndexReadSession index,
            CURSOR cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws KernelException;

    void entityIndexScan(KernelTransaction tx, IndexReadSession index, CURSOR cursor, IndexQueryConstraints constraints)
            throws KernelException;

    void createEntityIndex(
            Transaction tx, String entityToken, String propertyKey, String indexName, IndexType indexType);

    void createCompositeEntityIndex(
            Transaction tx,
            String entityToken,
            String propertyKey1,
            String propertyKey2,
            String indexName,
            IndexType indexType);

    void entitySetProperty(KernelTransaction tx, long entityId, int propId, String value) throws KernelException;

    void entitySetProperty(KernelTransaction tx, long entityId, int propId, Value value) throws KernelException;

    int entityTokenId(KernelTransaction tx, String tokenName);

    SchemaDescriptor schemaDescriptor(int tokenId, int propId);

    Value getPropertyValueFromStore(KernelTransaction tx, CursorFactory cursorFactory, long reference);

    void entityDelete(KernelTransaction tx, long reference) throws InvalidTransactionTypeKernelException;

    void entityRemoveToken(KernelTransaction tx, long entityId, int tokenId) throws KernelException;

    void entityAddToken(KernelTransaction tx, long entityId, int tokenId) throws KernelException;

    long entityCreateNew(KernelTransaction tx, int tokenId) throws KernelException;
}
