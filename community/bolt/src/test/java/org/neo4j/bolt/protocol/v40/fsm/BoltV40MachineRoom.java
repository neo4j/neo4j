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
package org.neo4j.bolt.protocol.v40.fsm;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.testing.assertions.StateMachineAssertions.assertThat;

import java.time.Clock;
import org.mockito.Mockito;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPIImpl;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPI;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.protocol.common.transaction.statement.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.testing.messages.BoltV40Messages;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.transaction.StatementProcessorTxManager;
import org.neo4j.kernel.database.DefaultDatabaseResolver;

/**
 * Helpers for testing the {@link StateMachine}.
 */
public class BoltV40MachineRoom {
    private BoltV40MachineRoom() {}

    public static StateMachine newMachine() {
        return newMachine(Mockito.mock(StateMachineSPIImpl.class, RETURNS_MOCKS));
    }

    public static StateMachine newMachine(Connection connection, StateMachineSPIImpl spi) {
        return new StateMachineV40(
                spi,
                connection,
                Clock.systemUTC(),
                mock(DefaultDatabaseResolver.class),
                new StatementProcessorTxManager());
    }

    public static StateMachine newMachine(Connection connection) {
        return newMachine(connection, mock(StateMachineSPIImpl.class, RETURNS_MOCKS));
    }

    public static StateMachine newMachine(StateMachineSPIImpl spi) {
        return newMachine(ConnectionMockFactory.newInstance(), spi);
    }

    public static StateMachine newMachineWithMockedTxManager() {
        return newMachineWithMockedTxManager(Mockito.mock(StateMachineSPIImpl.class, RETURNS_MOCKS));
    }

    public static StateMachine newMachineWithMockedTxManager(StateMachineSPIImpl spi) {
        return new StateMachineV40(
                spi,
                ConnectionMockFactory.newInstance(),
                Clock.systemUTC(),
                mock(DefaultDatabaseResolver.class),
                mock(StatementProcessorTxManager.class));
    }

    public static void initTransaction(StateMachine fsm) throws BoltConnectionFatality, BoltIOException {
        init(fsm);
        runBegin(fsm);
    }

    public static StateMachine newMachineWithTransactionSPI(TransactionStateMachineSPI transactionSPI)
            throws BoltConnectionFatality, BoltIOException {
        var spi = mock(StateMachineSPI.class, RETURNS_MOCKS);
        var transactionSPIProvider = mock(TransactionStateMachineSPIProvider.class);

        when(transactionSPIProvider.getTransactionStateMachineSPI(
                        any(String.class), any(StatementProcessorReleaseManager.class), any(String.class)))
                .thenReturn(transactionSPI);
        when(spi.transactionStateMachineSPIProvider()).thenReturn(transactionSPIProvider);

        var machine = new StateMachineV40(
                spi,
                ConnectionMockFactory.newInstance(),
                Clock.systemUTC(),
                mock(DefaultDatabaseResolver.class),
                new StatementProcessorTxManager());
        init(machine);
        return machine;
    }

    public static StateMachine init(StateMachine machine) throws BoltConnectionFatality {
        machine.process(BoltV40Messages.hello(), nullResponseHandler());
        return machine;
    }

    public static void reset(Connection connection, StateMachine machine, ResponseHandler handler)
            throws BoltConnectionFatality {
        when(connection.isInterrupted()).thenReturn(true);

        machine.interrupt();
        machine.process(BoltV40Messages.reset(), handler);

        when(connection.isInterrupted()).thenReturn(false);
    }

    private static void runBegin(StateMachine machine) throws BoltConnectionFatality {
        machine.process(BoltV40Messages.begin(), nullResponseHandler());
        assertThat(machine).hasTransaction();
    }
}
