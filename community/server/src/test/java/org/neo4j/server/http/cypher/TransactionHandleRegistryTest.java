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
package org.neo4j.server.http.cypher;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class TransactionHandleRegistryTest {
    @Test
    void shouldGenerateTransactionId() {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ofMillis(0), logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);

        // when
        long id1 = registry.begin(handle);
        long id2 = registry.begin(handle);

        // then
        assertNotEquals(id1, id2);
        assertThat(logProvider).doesNotHaveAnyLogs();

        verify(memoryPool, times(2)).reserveHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void shouldStoreSuspendedTransaction() throws Exception {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ofMillis(0), logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);

        long id = registry.begin(handle);

        // When
        registry.release(id, handle);
        TransactionHandle acquiredHandle = registry.acquire(id);

        // Then
        assertSame(handle, acquiredHandle);
        assertThat(logProvider).doesNotHaveAnyLogs();

        var inOrder = inOrder(memoryPool);
        inOrder.verify(memoryPool).reserveHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).reserveHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).releaseHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void acquiringATransactionThatHasAlreadyBeenAcquiredShouldThrowInvalidConcurrentTransactionAccess()
            throws Exception {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ofMillis(0), logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);

        long id = registry.begin(handle);
        registry.release(id, handle);
        registry.acquire(id);

        // When
        assertThrows(InvalidConcurrentTransactionAccess.class, () -> registry.acquire(id));

        // then
        assertThat(logProvider).doesNotHaveAnyLogs();

        var inOrder = inOrder(memoryPool);
        inOrder.verify(memoryPool).reserveHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).reserveHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).releaseHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void acquiringANonExistentTransactionShouldThrowErrorInvalidTransactionId() {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ofMillis(0), logProvider, memoryPool);

        long madeUpTransactionId = 1337;

        // When
        assertThrows(InvalidTransactionId.class, () -> registry.acquire(madeUpTransactionId));

        // then
        assertThat(logProvider).doesNotHaveAnyLogs();

        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void transactionsShouldBeEvictedWhenUnusedLongerThanTimeout() throws Exception {
        // Given
        FakeClock clock = Clocks.fakeClock();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, Duration.ofMillis(0), logProvider, memoryPool);
        TransactionHandle oldTx = mock(TransactionHandle.class);
        TransactionHandle newTx = mock(TransactionHandle.class);
        TransactionHandle handle = mock(TransactionHandle.class);

        long txId1 = registry.begin(handle);
        long txId2 = registry.begin(handle);

        // And given one transaction was stored one minute ago, and another was stored just now
        registry.release(txId1, oldTx);
        clock.forward(1, TimeUnit.MINUTES);
        registry.release(txId2, newTx);

        // When
        registry.rollbackSuspendedTransactionsIdleSince(clock.millis() - 1000);

        // Then
        assertThat(registry.acquire(txId2)).isEqualTo(newTx);

        // And then the other should have been evicted
        assertThrows(InvalidTransactionId.class, () -> registry.acquire(txId1));

        assertThat(logProvider)
                .forClass(TransactionHandleRegistry.class)
                .forLevel(INFO)
                .containsMessages(
                        "Transaction with id 1 has been automatically rolled " + "back due to transaction timeout.");

        var inOrder = inOrder(memoryPool);
        inOrder.verify(memoryPool, times(2)).reserveHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool, times(2)).reserveHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).releaseHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).releaseHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void expiryTimeShouldBeSetToCurrentTimePlusTimeout() throws Exception {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        var memoryPool = mock(MemoryPool.class);
        int timeoutLength = 123;

        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, Duration.ofMillis(timeoutLength), logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);

        long id = registry.begin(handle);

        // When
        long timesOutAt = registry.release(id, handle);

        // Then
        assertThat(timesOutAt).isEqualTo(clock.millis() + timeoutLength);

        // And when
        clock.forward(1337, TimeUnit.MILLISECONDS);
        registry.acquire(id);
        timesOutAt = registry.release(id, handle);

        // Then
        assertThat(timesOutAt).isEqualTo(clock.millis() + timeoutLength);
    }

    @Test
    void shouldProvideInterruptHandlerForActiveTransaction() throws TransactionLifecycleException {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        var memoryPool = mock(MemoryPool.class);
        int timeoutLength = 123;

        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, Duration.ofMillis(timeoutLength), logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);

        // Active Tx in Registry
        long id = registry.begin(handle);

        // When
        registry.terminate(id);

        // Then
        verify(handle).terminate();
        verifyNoMoreInteractions(handle);

        verify(memoryPool).reserveHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        verify(memoryPool).releaseHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void shouldProvideInterruptHandlerForSuspendedTransaction() throws TransactionLifecycleException {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        var memoryPool = mock(MemoryPool.class);
        int timeoutLength = 123;

        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, Duration.ofMillis(timeoutLength), logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);

        // Suspended Tx in Registry
        long id = registry.begin(handle);
        registry.release(id, handle);

        // When
        registry.terminate(id);

        // Then
        verify(handle).terminate();
        verifyNoMoreInteractions(handle);

        var inOrder = inOrder(memoryPool);
        inOrder.verify(memoryPool).reserveHeap(TransactionHandleRegistry.ACTIVE_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).reserveHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verify(memoryPool).releaseHeap(TransactionHandleRegistry.SUSPENDED_TRANSACTION_SHALLOW_SIZE);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void gettingInterruptHandlerForUnknownIdShouldThrowErrorInvalidTransactionId() {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        var memoryPool = mock(MemoryPool.class);
        int timeoutLength = 123;

        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, Duration.ofMillis(timeoutLength), logProvider, memoryPool);

        // When
        assertThrows(InvalidTransactionId.class, () -> registry.terminate(456));

        verifyNoMoreInteractions(memoryPool);
    }

    @Test
    void sameUserShouldBeAbleToAcquireTransaction() throws Exception {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ZERO, logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);
        LoginContext loginContext = mockLoginContext("Johannes");
        when(handle.loginContext()).thenReturn(loginContext);

        long id = registry.begin(handle);
        registry.release(id, handle);

        // When
        TransactionHandle acquiredHandle = registry.acquire(id, loginContext);

        // then
        assertSame(handle, acquiredHandle);
    }

    @Test
    void differentUserShouldNotBeAbleToAcquireTransaction() throws Exception {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ZERO, logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);
        LoginContext owningUser = mockLoginContext("Johannes");
        when(handle.loginContext()).thenReturn(owningUser);

        long id = registry.begin(handle);
        registry.release(id, handle);

        // When & Then
        LoginContext naughtyUser = mockLoginContext("Dr. Evil");
        assertThrows(InvalidTransactionId.class, () -> registry.acquire(id, naughtyUser));
    }

    @Test
    void shouldRetrieveHandlerLoginContext() throws Exception {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(Clocks.fakeClock(), Duration.ZERO, logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);
        LoginContext owningUser = mockLoginContext("Johannes");
        when(handle.loginContext()).thenReturn(owningUser);

        long id = registry.begin(handle);

        // When
        LoginContext actualLoginContext = registry.getLoginContextForTransaction(id);

        // Then
        assertSame(owningUser, actualLoginContext);
    }

    @Test
    void differentUserShouldNotBeAbleToTerminateTransaction() {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        FakeClock clock = Clocks.fakeClock();
        var timeoutLength = Duration.ofMillis(123);

        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, timeoutLength, logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);
        LoginContext loginContext = mockLoginContext("Johannes");
        when(handle.loginContext()).thenReturn(loginContext);

        // Active Tx in Registry
        long id = registry.begin(handle);

        // When & Then
        LoginContext otherUser = mockLoginContext("Dr. Evil");
        assertThrows(InvalidTransactionId.class, () -> registry.terminate(id, otherUser));
    }

    @Test
    void sameUserShouldBeAbleToTerminateTransaction() throws TransactionLifecycleException {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        var memoryPool = mock(MemoryPool.class);
        FakeClock clock = Clocks.fakeClock();
        var timeoutLength = Duration.ofMillis(123);

        TransactionHandleRegistry registry =
                new TransactionHandleRegistry(clock, timeoutLength, logProvider, memoryPool);
        TransactionHandle handle = mock(TransactionHandle.class);
        LoginContext loginContext = mockLoginContext("Johannes");
        when(handle.loginContext()).thenReturn(loginContext);

        // Active Tx in Registry
        long id = registry.begin(handle);

        // When
        registry.terminate(id, loginContext);

        // Then
        verify(handle, times(1)).terminate();
        verify(handle).loginContext();
        verifyNoMoreInteractions(handle);
    }

    private LoginContext mockLoginContext(String userName) {
        LoginContext loginContext = mock(LoginContext.class);
        AuthSubject authSubject = mock(AuthSubject.class);
        when(loginContext.subject()).thenReturn(authSubject);
        when(authSubject.authenticatedUser()).thenReturn(userName);
        return loginContext;
    }
}
