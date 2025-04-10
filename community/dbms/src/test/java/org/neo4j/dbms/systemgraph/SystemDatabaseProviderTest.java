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
package org.neo4j.dbms.systemgraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider.SystemDatabasePanickedException;
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider.SystemDatabaseUnavailableException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.TransactionIdStore;

class SystemDatabaseProviderTest {
    private int queryCount;
    private int executeCount;

    private final Object queryResult = new Object();
    private final Function<Transaction, Object> query = tx -> {
        queryCount++;
        return queryResult;
    };
    private final Consumer<Transaction> execute = tx -> {
        executeCount++;
    };

    private final GraphDatabaseAPI database = mock(GraphDatabaseAPI.class);
    private final DatabaseHealth health = mock(DatabaseHealth.class);
    private final Transaction transaction = mock(Transaction.class);
    private SystemDatabaseProvider provider;

    @BeforeEach
    void setup() {
        when(database.beginTx()).thenReturn(transaction);
        when(database.getDependencyResolver()).thenReturn(Dependencies.dependenciesOf(health));
        provider = () -> Optional.of(database);
    }

    @Test
    void whenDatabaseNotPresent() {
        // when
        provider = Optional::empty;

        // then
        assertThat(provider.optionalDatabase()).isEmpty();
        assertThatThrownBy(provider::database).isInstanceOf(SystemDatabaseUnavailableException.class);
        assertThatThrownBy(() -> provider.query(query)).isInstanceOf(SystemDatabaseUnavailableException.class);
        assertThatThrownBy(() -> provider.execute(execute)).isInstanceOf(SystemDatabaseUnavailableException.class);
        assertThat(provider.queryIfAvailable(query)).isEmpty();
        assertThat(provider.dependency(DatabaseHealth.class)).isEmpty();
        assertThat(provider.dependency(TransactionIdStore.class)).isEmpty();

        verifyNoInteractions(database, health, transaction);
        assertThat(queryCount).isZero();
        assertThat(executeCount).isZero();
    }

    @Test
    void whenSystemDatabaseNotAvailable() {
        // when
        when(database.isAvailable(anyLong())).thenReturn(false);
        when(health.hasNoPanic()).thenReturn(true);

        // then - optionalDatabase/database
        assertThat(provider.optionalDatabase()).hasValue(database);
        assertThat(provider.database()).isSameAs(database);

        verifyNoInteractions(database, health, transaction);

        // then - query/execute
        assertThatThrownBy(() -> provider.query(query)).isInstanceOf(SystemDatabaseUnavailableException.class);
        assertThatThrownBy(() -> provider.execute(execute)).isInstanceOf(SystemDatabaseUnavailableException.class);

        verify(database, times(2)).isAvailable(1000);
        verify(database, times(2)).getDependencyResolver();
        verify(health, times(2)).hasNoPanic();
        verifyNoMoreInteractions(database, health);
        clearInvocations(database, health);
        verifyNoInteractions(transaction);

        // then - queryIfAvailable
        assertThat(provider.queryIfAvailable(query)).isEmpty();

        verify(database).isAvailable(0);
        verifyNoMoreInteractions(database);
        clearInvocations(database);
        verifyNoInteractions(health, transaction);

        // then - dependency
        assertThat(provider.dependency(DatabaseHealth.class)).hasValue(health);
        assertThat(provider.dependency(TransactionIdStore.class)).isEmpty();
        verify(database, times(2)).getDependencyResolver();
        verifyNoMoreInteractions(database);
        verifyNoInteractions(health, transaction);

        assertThat(queryCount).isZero();
        assertThat(executeCount).isZero();
    }

    @Test
    void whenSystemDatabaseInPanic() {
        // when
        when(database.isAvailable(anyLong())).thenReturn(false);
        when(health.hasNoPanic()).thenReturn(false);

        // then - optionalDatabase/database
        assertThat(provider.optionalDatabase()).hasValue(database);
        assertThat(provider.database()).isSameAs(database);

        verifyNoInteractions(database, health, transaction);

        // then - query/execute
        assertThatThrownBy(() -> provider.query(query)).isInstanceOf(SystemDatabasePanickedException.class);
        assertThatThrownBy(() -> provider.execute(execute)).isInstanceOf(SystemDatabasePanickedException.class);

        verify(database, times(2)).isAvailable(1000);
        verify(database, times(2)).getDependencyResolver();
        verify(health, times(2)).hasNoPanic();
        verifyNoMoreInteractions(database, health);
        clearInvocations(database, health);
        verifyNoInteractions(transaction);

        // then - queryIfAvailable
        assertThat(provider.queryIfAvailable(query)).isEmpty();

        verify(database).isAvailable(0);
        verifyNoMoreInteractions(database);
        clearInvocations(database);
        verifyNoInteractions(health, transaction);

        // then - dependency
        assertThat(provider.dependency(DatabaseHealth.class)).hasValue(health);
        assertThat(provider.dependency(TransactionIdStore.class)).isEmpty();
        verify(database, times(2)).getDependencyResolver();
        verifyNoMoreInteractions(database);
        verifyNoInteractions(health, transaction);

        assertThat(queryCount).isZero();
        assertThat(executeCount).isZero();
    }

    @Test
    void whenAllGood() {
        // when
        when(database.isAvailable(anyLong())).thenReturn(true);

        // then - optionalDatabase/database
        assertThat(provider.optionalDatabase()).hasValue(database);
        assertThat(provider.database()).isSameAs(database);

        verifyNoInteractions(database, health, transaction);

        // then - query/execute
        assertThat(provider.query(query)).isSameAs(queryResult);
        provider.execute(execute);

        verify(database, times(2)).isAvailable(1000);
        verify(database, times(2)).beginTx();
        verify(transaction, times(2)).commit();
        verify(transaction, times(2)).close();
        verifyNoMoreInteractions(database, transaction);
        clearInvocations(database, transaction);
        verifyNoInteractions(health);

        // then - queryIfAvailable
        assertThat(provider.queryIfAvailable(query)).hasValue(queryResult);

        verify(database).isAvailable(0);
        verify(database).beginTx();
        verify(transaction).commit();
        verify(transaction).close();
        verifyNoMoreInteractions(database, transaction);
        clearInvocations(database, transaction);
        verifyNoInteractions(health);

        // then - dependency
        assertThat(provider.dependency(DatabaseHealth.class)).hasValue(health);
        assertThat(provider.dependency(TransactionIdStore.class)).isEmpty();
        verify(database, times(2)).getDependencyResolver();
        verifyNoMoreInteractions(database);
        verifyNoInteractions(health, transaction);

        assertThat(queryCount).isEqualTo(2);
        assertThat(executeCount).isEqualTo(1);
    }

    @Test
    void whenQueryThrows() {
        // given
        var ex0 = new RuntimeException();
        var ex1 = new RuntimeException();
        var ex2 = new RuntimeException();

        // when
        when(database.isAvailable(anyLong())).thenReturn(true);

        // then - query/execute
        assertThatThrownBy(() -> provider.query(tx -> {
                    throw ex0;
                }))
                .isEqualTo(ex0);
        assertThatThrownBy(() -> provider.execute(tx -> {
                    throw ex1;
                }))
                .isEqualTo(ex1);
        assertThatThrownBy(() -> provider.queryIfAvailable(tx -> {
                    throw ex2;
                }))
                .isEqualTo(ex2);

        verify(database, times(2)).isAvailable(1000);
        verify(database, times(1)).isAvailable(0);
        verify(database, times(3)).beginTx();
        verify(transaction, times(3)).close();
        verifyNoMoreInteractions(database, transaction);
        verifyNoInteractions(health);
    }
}
