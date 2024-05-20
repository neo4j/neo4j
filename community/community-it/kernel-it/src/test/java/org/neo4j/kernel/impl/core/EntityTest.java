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
package org.neo4j.kernel.impl.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ImpermanentDbmsExtension
public abstract class EntityTest {
    @Inject
    static GraphDatabaseAPI db;

    protected abstract long createEntity(Transaction tx);

    protected abstract Entity lookupEntity(Transaction transaction, long id);

    @Test
    void shouldListAllProperties() {
        // Given
        Map<String, Object> properties = new HashMap<>();
        properties.put("boolean", true);
        properties.put("short_string", "abc");
        properties.put(
                "string",
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVW"
                        + "XYZabcdefghijklmnopqrstuvwxyz");
        properties.put("long", Long.MAX_VALUE);
        properties.put("short_array", new long[] {1, 2, 3, 4});
        properties.put("array", new long[] {
            Long.MAX_VALUE - 1,
            Long.MAX_VALUE - 2,
            Long.MAX_VALUE - 3,
            Long.MAX_VALUE - 4,
            Long.MAX_VALUE - 5,
            Long.MAX_VALUE - 6,
            Long.MAX_VALUE - 7,
            Long.MAX_VALUE - 8,
            Long.MAX_VALUE - 9,
            Long.MAX_VALUE - 10,
            Long.MAX_VALUE - 11
        });

        long containerId;

        try (Transaction tx = db.beginTx()) {
            containerId = createEntity(tx);
            Entity container = lookupEntity(tx, containerId);

            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                container.setProperty(entry.getKey(), entry.getValue());
            }

            tx.commit();
        }

        // When
        Map<String, Object> listedProperties;
        try (Transaction tx = db.beginTx()) {
            listedProperties = lookupEntity(tx, containerId).getAllProperties();
            tx.commit();
        }

        // Then
        assertEquals(properties.size(), listedProperties.size());
        for (String key : properties.keySet()) {
            assertThat(properties.get(key)).isEqualTo(listedProperties.get(key));
        }
    }

    static InternalTransaction mockedTransactionWithDepletedTokens() throws KernelException {
        var internalTransaction = mock(InternalTransaction.class);
        var ktx = mock(KernelTransaction.class);
        var tokenWrite = mock(TokenWrite.class);
        when(ktx.tokenWrite()).thenReturn(tokenWrite);
        when(tokenWrite.labelGetOrCreateForName(any()))
                .thenThrow(new TokenCapacityExceededKernelException(
                        new Exception("Just some cause"), TokenHolder.TYPE_LABEL));
        when(tokenWrite.propertyKeyGetOrCreateForName(any()))
                .thenThrow(new TokenCapacityExceededKernelException(
                        new Exception("Just some cause"), TokenHolder.TYPE_PROPERTY_KEY));
        when(tokenWrite.relationshipTypeGetOrCreateForName(any()))
                .thenThrow(new TokenCapacityExceededKernelException(
                        new Exception("Just some cause"), TokenHolder.TYPE_RELATIONSHIP_TYPE));
        when(internalTransaction.kernelTransaction()).thenReturn(ktx);
        return internalTransaction;
    }

    static InternalTransaction transactionWithTransientlyFailingTokenWrite() throws KernelException {
        var ktx = mock(KernelTransaction.class);
        var tokenWrite = mock(TokenWrite.class);
        when(ktx.tokenWrite()).thenReturn(tokenWrite);
        TransactionFailureException transientFailure =
                new TransactionFailureException(Status.Transaction.Outdated, new Exception("Just some cause"));
        doThrow(transientFailure).when(tokenWrite).labelGetOrCreateForNames(any(), any());
        when(tokenWrite.labelGetOrCreateForName(any())).thenThrow(transientFailure);
        when(tokenWrite.propertyKeyGetOrCreateForName(any())).thenThrow(transientFailure);
        when(tokenWrite.relationshipTypeGetOrCreateForName(any())).thenThrow(transientFailure);

        return new TransactionImpl(
                mock(TokenHolders.class),
                mock(TransactionalContextFactory.class),
                mock(DatabaseAvailabilityGuard.class),
                mock(QueryExecutionEngine.class),
                ktx,
                mock(ElementIdMapper.class));
    }
}
