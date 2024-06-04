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
package org.neo4j.dbms.database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.dbms.database.DatabaseDetailsExtras.EMPTY;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.database.TopologyInfoService.RequestedExtras;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.storageengine.api.TransactionIdStore;

class DefaultDatabaseDetailsExtrasProviderTest {
    private final DatabaseId databaseId = DatabaseIdFactory.from(UUID.randomUUID());
    private final StoreId storeId = new StoreId(1, 2, "engine", "format", 3, 4);
    private final ExternalStoreId externalStoreId = new ExternalStoreId(UUID.randomUUID());
    private final long lastCommittedTxId = 42;
    private final long lastAppendIndex = 44;
    private DatabaseContextProvider<? extends DatabaseContext> databaseContextProvider;
    private DefaultDatabaseDetailsExtrasProvider provider;

    @BeforeEach
    void setup() {
        //noinspection unchecked
        databaseContextProvider = mock(DatabaseContextProvider.class);
        provider = new DefaultDatabaseDetailsExtrasProvider(databaseContextProvider);

        var storeIdProvider = mock(StoreIdProvider.class);
        when(storeIdProvider.getExternalStoreId()).thenReturn(externalStoreId);
        var transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn(lastCommittedTxId);
        var appendIndexProvider = mock(AppendIndexProvider.class);
        when(appendIndexProvider.getLastAppendIndex()).thenReturn(lastAppendIndex);

        var dependencies = new Dependencies();
        dependencies.satisfyDependency(storeIdProvider);
        dependencies.satisfyDependency(transactionIdStore);
        dependencies.satisfyDependency(appendIndexProvider);
        var database = mock(Database.class);
        when(database.getStoreId()).thenReturn(storeId);
        when(database.isStarted()).thenReturn(true);
        var context = mock(DatabaseContext.class);
        when(context.database()).thenReturn(database);
        when(context.dependencies()).thenReturn(dependencies);
        when(databaseContextProvider.getDatabaseContext(databaseId)).thenAnswer(args -> Optional.of(context));
    }

    @Test
    void shouldReturnEmptyIfNothingRequested() {
        // when
        var result = provider.extraDetails(databaseId, RequestedExtras.NONE);

        // then
        assertThat(result).isSameAs(EMPTY);
        verifyNoInteractions(databaseContextProvider);
    }

    @Test
    void shouldReturnEmptyIfDatabaseNotPresent() {
        // given
        when(databaseContextProvider.getDatabaseContext(databaseId)).thenAnswer(args -> Optional.empty());

        // when
        var result = provider.extraDetails(databaseId, RequestedExtras.ALL);

        // then
        assertThat(result)
                .isEqualTo(new DatabaseDetailsExtras(
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
        verify(databaseContextProvider).getDatabaseContext(databaseId);
    }

    @Test
    void shouldReturnIfStoreIdRequested() {
        // when
        var result = provider.extraDetails(databaseId, new RequestedExtras(false, true));

        // then
        assertThat(result)
                .isEqualTo(new DatabaseDetailsExtras(
                        Optional.empty(), Optional.empty(), Optional.of(storeId), Optional.of(externalStoreId)));
        verify(databaseContextProvider).getDatabaseContext(databaseId);
    }

    @Test
    void shouldReturnIfTransactionIdRequested() {
        // when
        var result = provider.extraDetails(databaseId, new RequestedExtras(true, false));

        // then
        assertThat(result)
                .isEqualTo(new DatabaseDetailsExtras(
                        Optional.of(lastCommittedTxId),
                        Optional.of(lastAppendIndex),
                        Optional.empty(),
                        Optional.empty()));
        verify(databaseContextProvider).getDatabaseContext(databaseId);
    }
}
