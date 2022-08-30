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
package org.neo4j.bolt.protocol.common.connector.connection;

import io.netty.channel.Channel;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;

/**
 * Provides a generic base implementation for connections.
 */
public abstract class AbstractConnection implements Connection {
    private final Connector connector;

    protected final String id;
    protected final Channel channel;
    private final long connectedAt;
    protected final MemoryTracker memoryTracker;
    protected final InternalLog log;
    protected final Log userLog;

    private final Lock listenerLock = new ReentrantLock();
    private final List<ConnectionListener> listeners = new CopyOnWriteArrayList<>();

    protected final AtomicReference<BoltProtocol> protocol = new AtomicReference<>();
    protected volatile StateMachine fsm;

    private final AtomicReference<LoginContext> loginContext = new AtomicReference<>();
    private volatile LoginContext impersonationContext;

    private volatile BoltConnectionInfo connectionInfo;
    private volatile String username;
    private volatile String userAgent;
    private String defaultDatabase;
    private String impersonatedDefaultDatabase;

    public AbstractConnection(
            Connector connector,
            String id,
            Channel channel,
            long connectedAt,
            MemoryTracker memoryTracker,
            LogService logService) {
        this.connector = connector;
        this.id = id;
        this.channel = channel;
        this.connectedAt = connectedAt;
        this.memoryTracker = memoryTracker;
        this.log = logService.getInternalLog(this.getClass());
        this.userLog = logService.getUserLog(this.getClass());
    }

    @Override
    public Connector connector() {
        return this.connector;
    }

    @Override
    public String connectorId() {
        return this.connector.id();
    }

    @Override
    public Channel channel() {
        return this.channel;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public ClientConnectionInfo info() {
        var connectionInfo = this.connectionInfo;
        if (connectionInfo == null) {
            throw new IllegalStateException("Connection " + this.id + " has yet to be authenticated");
        }

        return connectionInfo;
    }

    @Override
    public long connectTime() {
        return this.connectedAt;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return this.memoryTracker;
    }

    @Override
    public void registerListener(ConnectionListener listener) {
        this.listenerLock.lock();
        try {
            if (this.listeners.contains(listener)) {
                return;
            }

            this.listeners.add(listener);

            listener.onListenerAdded();
        } finally {
            this.listenerLock.unlock();
        }
    }

    @Override
    public void removeListener(ConnectionListener listener) {
        this.listenerLock.lock();
        try {
            this.listeners.remove(listener);

            listener.onListenerRemoved();
        } finally {
            this.listenerLock.unlock();
        }
    }

    @Override
    public void notifyListeners(Consumer<ConnectionListener> consumer) {
        this.listeners.forEach(consumer);
    }

    @Override
    public void notifyListenersSafely(String eventName, Consumer<ConnectionListener> notifierFunction) {
        this.listeners.forEach(listener -> {
            try {
                notifierFunction.accept(listener);
            } catch (Throwable ex) {
                log.error(
                        "[" + this.id + "] Failed to publish " + eventName + " event to listener "
                                + listener.getClass().getSimpleName(),
                        ex);
            }
        });
    }

    @Override
    public BoltProtocol protocol() {
        return this.protocol.get();
    }

    @Override
    public void selectProtocol(BoltProtocol protocol) {
        Objects.requireNonNull(protocol, "protocol");

        if (!this.protocol.compareAndSet(null, protocol)) {
            throw new IllegalStateException("Protocol has already been selected for connection " + this.id);
        }

        var fsm = protocol.createStateMachine(this);
        this.fsm = fsm;

        this.notifyListeners(listener -> listener.onStateMachineInitialized(fsm));
    }

    @Override
    public StateMachine fsm() {
        var fsm = this.fsm;
        if (fsm == null) {
            throw new IllegalStateException("Connection has yet to select a protocol version");
        }

        return fsm;
    }

    @Override
    public LoginContext loginContext() {
        var impersonationContext = this.impersonationContext;
        if (impersonationContext != null) {
            return impersonationContext;
        }

        return this.loginContext.get();
    }

    @Override
    public AuthenticationFlag authenticate(Map<String, Object> token, String userAgent) throws AuthenticationException {
        this.userAgent = userAgent;
        this.connectionInfo = new BoltConnectionInfo(this.id, userAgent, this.clientAddress(), this.serverAddress());

        var result = this.connector().authentication().authenticate(token, this.info());

        var loginContext = result.getLoginContext();
        if (!this.loginContext.compareAndSet(null, loginContext)) {
            throw new IllegalStateException("Cannot re-authenticate connection");
        }

        this.updateUser(loginContext.subject().authenticatedUser(), userAgent);
        log.debug(
                "[%s] Authenticated with user '%s' (Credentials expired: %b)",
                this.id, loginContext.subject().authenticatedUser(), result.credentialsExpired());

        this.resolveDefaultDatabase();

        this.notifyListeners(listener -> listener.onAuthenticated(loginContext));

        if (result.credentialsExpired()) {
            return AuthenticationFlag.CREDENTIALS_EXPIRED;
        }
        return null;
    }

    @Override
    public void impersonate(String userToImpersonate) throws AuthenticationException {
        if (userToImpersonate == null) {
            // ignore the call entirely if there is no impersonation currently present on this connection to avoid any
            // unnecessary notifications to the application log as well as connection-level listeners
            if (this.impersonationContext == null) {
                return;
            }

            log.debug("[%s] Disabling impersonation", this.id);
            this.impersonationContext = null;

            var defaultDatabase = this.defaultDatabase;
            var impersonatedDefaultDatabase = this.impersonatedDefaultDatabase;
            this.impersonatedDefaultDatabase = null;

            if (!Objects.equals(impersonatedDefaultDatabase, defaultDatabase)) {
                this.notifyListeners(listener -> listener.onDefaultDatabaseSelected(defaultDatabase));
            }

            this.notifyListeners(ConnectionListener::onUserImpersonationCleared);
            return;
        }

        var loginContext = this.loginContext.get();
        if (loginContext == null) {
            throw new IllegalStateException("Cannot impersonate without prior authentication");
        }

        log.debug("[%s] Enabling impersonation of user '%s'", this.id, userToImpersonate);
        this.impersonationContext = this.connector.authentication().impersonate(loginContext, userToImpersonate);

        this.resolveDefaultDatabase();

        this.notifyListeners(listener -> listener.onUserImpersonated(this.impersonationContext));
    }

    @Override
    public SocketAddress serverAddress() {
        return this.channel.localAddress();
    }

    @Override
    public SocketAddress clientAddress() {
        return this.channel.remoteAddress();
    }

    @Override
    public String username() {
        return this.username;
    }

    @Override
    public String userAgent() {
        return this.userAgent;
    }

    // TODO: Remove this function?
    @Override
    public void updateUser(String username, String userAgent) {
        this.username = username;
    }

    @Override
    public String selectedDefaultDatabase() {
        var impersonatedDefaultDatabase = this.impersonatedDefaultDatabase;
        if (impersonatedDefaultDatabase != null) {
            return this.impersonatedDefaultDatabase;
        }

        return this.defaultDatabase;
    }

    @Override
    public void resolveDefaultDatabase() {
        var loginContext = this.loginContext();
        if (loginContext == null) {
            throw new IllegalStateException("Cannot resolve default database: Connection has not been authenticated");
        }

        var db = this.connector()
                .defaultDatabaseResolver()
                .defaultDatabase(this.loginContext().subject().executingUser());

        String previousDatabase;
        if (loginContext.impersonating()) {
            previousDatabase = this.impersonatedDefaultDatabase;
            this.impersonatedDefaultDatabase = db;
        } else {
            previousDatabase = this.defaultDatabase;

            this.impersonatedDefaultDatabase = null;
            this.defaultDatabase = db;
        }

        if (!Objects.equals(previousDatabase, db)) {
            this.notifyListeners(listener -> listener.onDefaultDatabaseSelected(db));
        }
    }

    @Override
    public String toString() {
        return "AbstractConnection{" + "connector="
                + connector + ", channel="
                + channel + ", connectedAt="
                + connectedAt + ", protocol="
                + protocol + ", fsm="
                + fsm + ", loginContext="
                + loginContext + ", impersonationContext="
                + impersonationContext + ", connectionInfo="
                + connectionInfo + ", username='"
                + username + '\'' + ", userAgent='"
                + userAgent + '\'' + ", defaultDatabase='"
                + defaultDatabase + '\'' + '}';
    }
}
