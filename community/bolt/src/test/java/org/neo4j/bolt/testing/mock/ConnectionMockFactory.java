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
package org.neo4j.bolt.testing.mock;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.Job;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.connector.netty.AbstractNettyConnector.NettyConfiguration;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.io.pipeline.PipelineContext;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.packstream.io.value.PackstreamValueReader;

public class ConnectionMockFactory extends AbstractMockFactory<ConnectionHandle, ConnectionMockFactory> {
    private static final String DEFAULT_ID = "bolt-test-connection";

    private ConnectionMockFactory(String id) {
        super(ConnectionHandle.class);

        this.withId(id);
        this.withSelectedDefaultDatabase("neo4j");
        this.withConnector(ConnectorMockFactory.newInstance());
    }

    public static ConnectionMockFactory newFactory(String id) {
        return new ConnectionMockFactory(id);
    }

    public static ConnectionMockFactory newFactory() {
        return newFactory(DEFAULT_ID);
    }

    public static ConnectionHandle newInstance() {
        return newFactory().build();
    }

    public static ConnectionHandle newInstance(String id, Consumer<ConnectionMockFactory> configurer) {
        var factory = newFactory(id);
        configurer.accept(factory);
        return factory.build();
    }

    public static ConnectionHandle newInstance(String id) {
        return newFactory(id).build();
    }

    public static ConnectionHandle newInstance(Consumer<ConnectionMockFactory> configurer) {
        return newInstance(DEFAULT_ID, configurer);
    }

    public ConnectionHandle attachTo(Channel channel, ChannelHandler... handlers) {
        var connection = this.build();
        Connection.setAttribute(channel, connection);

        channel.pipeline().addLast(handlers);

        return connection;
    }

    public ConnectionHandle attachToMock(Channel channel) {
        var attr = Mockito.mock(Attribute.class);

        var connection = this.build();
        Mockito.doReturn(connection).when(attr).get();

        Mockito.doReturn(attr).when(channel).attr(Connection.CONNECTION_ATTR);

        return connection;
    }

    public <C extends Channel> C createChannel(Supplier<C> factory, ChannelHandler... handlers) {
        var channel = factory.get();
        this.attachTo(channel);

        channel.pipeline().addLast(handlers);

        return channel;
    }

    public EmbeddedChannel createChannel(ChannelHandler... handlers) {
        return this.createChannel(EmbeddedChannel::new, handlers);
    }

    public ConnectionMockFactory withConnector(Connector connector) {
        return this.withStaticValue(Connection::connector, connector);
    }

    public ConnectionMockFactory withConnector(Consumer<ConnectorMockFactory> configurer) {
        return this.withStaticValue(Connection::connector, ConnectorMockFactory.newInstance(configurer));
    }

    public ConnectionMockFactory withConnector(String id, Consumer<ConnectorMockFactory> configurer) {
        return this.withStaticValue(Connection::connector, ConnectorMockFactory.newInstance(id, configurer));
    }

    public ConnectionMockFactory withId(String id) {
        return this.withStaticValue(Connection::id, id);
    }

    public ConnectionMockFactory withMemoryTracker(MemoryTracker memoryTracker) {
        return this.withStaticValue(Connection::memoryTracker, memoryTracker);
    }

    public ConnectionMockFactory withChannel(Channel channel) {
        this.with(mock -> Mockito.doAnswer(invocation -> channel.write(invocation.getArgument(0)))
                .when(mock)
                .write(ArgumentMatchers.any()));
        this.with(mock -> Mockito.doAnswer(
                        invocation -> channel.write(invocation.getArgument(0), invocation.getArgument(1)))
                .when(mock)
                .write(ArgumentMatchers.any(), ArgumentMatchers.any()));

        this.with(mock -> Mockito.doAnswer(invocation -> channel.writeAndFlush(invocation.getArgument(0)))
                .when(mock)
                .writeAndFlush(ArgumentMatchers.any()));
        this.with(mock -> Mockito.doAnswer(
                        invocation -> channel.writeAndFlush(invocation.getArgument(0), invocation.getArgument(1)))
                .when(mock)
                .writeAndFlush(ArgumentMatchers.any(), ArgumentMatchers.any()));

        this.with(mock ->
                Mockito.doAnswer(invocation -> channel.flush()).when(mock).flush());

        return this.withStaticValue(Connection::channel, channel);
    }

    public ArgumentCaptor<ConnectionListener> withRegisterListenerCaptor() {
        return this.withArgumentCaptor(ConnectionListener.class, Connection::registerListener);
    }

    public ArgumentCaptor<ConnectionListener> withRemoveListenerCaptor() {
        return this.withArgumentCaptor(ConnectionListener.class, Connection::removeListener);
    }

    @SuppressWarnings("unchecked")
    public ArgumentCaptor<Consumer<ConnectionListener>> withNotifyListenersCaptor() {
        return (ArgumentCaptor) this.withArgumentCaptor(Consumer.class, Connection::notifyListeners);
    }

    public ConnectionMockFactory withProtocol(BoltProtocol protocol) {
        return this.withStaticValue(Connection::protocol, protocol);
    }

    public ArgumentCaptor<BoltProtocol> withProtocolCaptor() {
        var captor = this.withArgumentCaptor(BoltProtocol.class, Connection::selectProtocol);
        this.withAnswer(Connection::protocol, invocation -> captor.getValue());
        return captor;
    }

    public ConnectionMockFactory withValueReader(PackstreamValueReader<Connection> valueReader) {
        return this.withStaticValue(mock -> mock.valueReader(ArgumentMatchers.any()), valueReader);
    }

    public ConnectionMockFactory withWriterContext(PipelineContext ctx) {
        return this.withStaticValue(mock -> mock.writerContext(ArgumentMatchers.any()), ctx);
    }

    public ConnectionMockFactory withFSM(StateMachine fsm) {
        return this.withStaticValue(Connection::fsm, fsm);
    }

    public ConnectionMockFactory withAuthentication(Authentication authentication) {
        var loginContext = new AtomicReference<LoginContext>();

        this.with(connection -> Mockito.doAnswer(invocation -> loginContext.get())
                .when(connection)
                .loginContext());
        this.with(connection -> {
            try {
                Mockito.doAnswer(invocation -> {
                            var result = authentication.authenticate(
                                    invocation.getArgument(0),
                                    new BoltConnectionInfo(
                                            "bolt-test",
                                            "bolt-test",
                                            mock(SocketAddress.class),
                                            mock(SocketAddress.class),
                                            Collections.emptyMap()));
                            loginContext.set(result.getLoginContext());

                            if (result.credentialsExpired()) {
                                return AuthenticationFlag.CREDENTIALS_EXPIRED;
                            }

                            return null;
                        })
                        .when(connection)
                        .logon(anyMap());
            } catch (AuthenticationException ignore) {
                // Mock interaction - never happens
            }
        });

        return this;
    }

    public ConnectionMockFactory withAuthenticationFlag(AuthenticationFlag flag) {
        return this.withStaticValue(
                mock -> {
                    try {
                        return mock.logon(ArgumentMatchers.notNull());
                    } catch (AuthenticationException ignore) {
                        return null; // never happens
                    }
                },
                flag);
    }

    public ConnectionMockFactory withAuthenticationFlag(
            AuthenticationFlag flag, Supplier<Map<String, Object>> tokenMatcher) {
        return this.withStaticValue(
                mock -> {
                    try {
                        return mock.logon(tokenMatcher.get());
                    } catch (AuthenticationException ignore) {
                        return null; // never happens
                    }
                },
                flag);
    }

    public ConnectionMockFactory withAuthenticationFlag(AuthenticationFlag flag, Map<String, Object> token) {
        return this.withAuthenticationFlag(flag, () -> ArgumentMatchers.eq(token));
    }

    public ConnectionMockFactory withSelectedDefaultDatabase(String database) {
        return this.withStaticValue(Connection::selectedDefaultDatabase, database);
    }

    public ConnectionMockFactory withIdling(boolean idling) {
        return this.withStaticValue(Connection::isIdling, idling);
    }

    public ConnectionMockFactory withPendingJobs(boolean pendingJobs) {
        return this.withStaticValue(Connection::hasPendingJobs, pendingJobs);
    }

    public ArgumentCaptor<Job> withSubmissionCaptor() {
        return this.withArgumentCaptor(Job.class, Connection::submit);
    }

    public ConnectionMockFactory withInterrupted(boolean interrupted) {
        return this.withStaticValue(Connection::isInterrupted, interrupted);
    }

    public ConnectionMockFactory withInterruptedCaptor(AtomicInteger captor) {
        this.withAnswer(Connection::isInterrupted, invocation -> captor.get() != 0);

        this.withAnswer(Connection::interrupt, invocation -> {
            if (captor.incrementAndGet() == 1) {
                ((Connection) invocation.getMock()).fsm().interrupt();
            }
            return null; // void function
        });

        return this.withAnswer(Connection::reset, invocation -> {
            int oldValue;
            int newValue;
            do {
                oldValue = captor.get();
                if (oldValue == 0) {
                    newValue = 0;
                } else {
                    newValue = oldValue - 1;

                    if (newValue == 0) {
                        var connection = ((ConnectionHandle) invocation.getMock());
                        connection.closeTransaction();
                        connection.fsm().reset();
                    }
                }
            } while (!captor.compareAndSet(oldValue, newValue));

            return newValue == 0;
        });
    }

    public ConnectionMockFactory withTransaction(Transaction transaction) {
        return this.withStaticValue(ConnectionHandle::transaction, transaction);
    }

    public ConnectionMockFactory withTransactionManager(TransactionManager transactionManager) {
        var transaction = new AtomicReference<Transaction>();

        this.withAnswer(
                connection -> {
                    try {
                        connection.beginTransaction(
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any());
                    } catch (TransactionException ignore) {
                        // Stubbing invocation - never occurs
                    }
                },
                invocation -> {
                    var config = invocation.getArgument(6) == null
                            ? null
                            : invocation
                                    .getArgument(6, NotificationsConfig.class)
                                    .buildConfiguration(null);

                    var newTransaction = transactionManager.create(
                            invocation.getArgument(0),
                            (Connection) invocation.getMock(),
                            invocation.getArgument(1),
                            invocation.getArgument(2),
                            invocation.getArgument(3),
                            invocation.getArgument(4),
                            invocation.getArgument(5),
                            config);
                    transaction.set(newTransaction);
                    return newTransaction;
                });
        this.withAnswer(Connection::routingContext, invocation -> new RoutingContext(false, Map.of()));
        this.withAnswer(ConnectionHandle::transaction, invocation -> Optional.ofNullable(transaction.get()));
        this.withAnswer(
                connection -> {
                    try {
                        connection.closeTransaction();
                    } catch (TransactionException ignore) {
                        // Stubbing invocation - never occurs
                    }
                },
                invocation -> {
                    var currentTransaction = transaction.getAndSet(null);
                    if (currentTransaction != null) {
                        currentTransaction.close();
                    }

                    return null;
                });

        return this;
    }

    public ConnectionMockFactory withResetResult(boolean result) {
        return this.withStaticValue(Connection::reset, result);
    }

    public ConnectionMockFactory withClosing(boolean closing) {
        return this.withStaticValue(Connection::isClosing, closing);
    }

    public AtomicBoolean withClosingCaptor() {
        var closing = new AtomicBoolean();

        this.withAnswer(Connection::isClosing, invocation -> closing.get());
        this.withAnswer(Connection::close, invocation -> {
            closing.set(true);
            return null; // void function
        });

        return closing;
    }

    public ConnectionMockFactory withClosed(boolean closed) {
        if (closed) {
            this.withCloseFuture(CompletableFuture.completedFuture(null));
        }

        return this.withStaticValue(Connection::isClosed, closed);
    }

    public AtomicBoolean withCloseCaptor() {
        var closed = new AtomicBoolean();
        var future = new CompletableFuture<Void>();

        this.withAnswer(Connection::isClosed, invocation -> closed.get());
        this.withCloseFuture(future);
        this.withAnswer(Connection::close, invocation -> {
            closed.set(true);
            future.complete(null);
            return null; // void function
        });

        return closed;
    }

    public ConnectionMockFactory withCloseFuture(Future<Void> future) {
        return this.withStaticValue(Connection::closeFuture, future);
    }

    public ConnectionMockFactory withConfiguration(NettyConfiguration configuration) {
        return this.withConnector(factory -> factory.withConfiguration(configuration));
    }

    public ConnectionMockFactory withConfiguration(Consumer<ConnectorConfigurationMockFactory> configurer) {
        return this.withConnector(factory -> factory.withConfiguration(configurer));
    }
}
