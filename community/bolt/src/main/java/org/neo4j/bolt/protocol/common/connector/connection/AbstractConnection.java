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
package org.neo4j.bolt.protocol.common.connector.connection;

import io.netty.channel.Channel;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.neo4j.bolt.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.connector.connection.listener.ConnectionListener;
import org.neo4j.bolt.protocol.common.fsm.response.NetworkResponseHandler;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.protocol.io.pipeline.PipelineContext;
import org.neo4j.bolt.protocol.io.pipeline.WriterPipeline;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.value.PackstreamValueReader;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.storable.Value;

/**
 * Provides a generic base implementation for connections.
 */
public abstract class AbstractConnection implements ConnectionHandle {
    private final Connector connector;

    protected final String id;
    protected final Channel channel;
    private final long connectedAt;
    protected final MemoryTracker memoryTracker;
    protected final LogService logService;
    protected final InternalLog log;
    protected final Log userLog;

    private final Lock listenerLock = new ReentrantLock();
    private final List<ConnectionListener> listeners = new CopyOnWriteArrayList<>();

    protected final AtomicReference<BoltProtocol> protocol = new AtomicReference<>();
    private final AtomicReference<Set<Feature>> features = new AtomicReference<>(null);
    protected volatile StateMachine fsm;
    // TODO: Switch to immutable writer pipeline implementation?
    protected volatile WriterPipeline writerPipeline;
    protected final AtomicReference<StructRegistry<Connection, Value>> structRegistry = new AtomicReference<>();
    protected volatile ResponseHandler responseHandler;

    private final AtomicReference<LoginContext> loginContext = new AtomicReference<>();
    private volatile LoginContext impersonationContext;

    private volatile RoutingContext routingContext;
    private volatile BoltConnectionInfo connectionInfo;
    private volatile String username;
    private volatile String userAgent;
    private volatile Map<String, String> boltAgent;
    private String defaultDatabase;
    private String impersonatedDefaultDatabase;
    protected NotificationsConfig notificationsConfig;

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

        this.logService = logService;
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

        // initialize the writer pipeline and struct registry for use with the desired protocol
        var pipeline = new WriterPipeline(this);
        var structRegistry = StructRegistry.<Connection, Value>builder();

        // update the writer pipeline to include Bolt protocol specific writer implementations for supported structures
        // within the protocol
        protocol.registerStructWriters(pipeline);
        protocol.registerStructReaders(structRegistry);

        this.writerPipeline = pipeline;
        this.structRegistry.set(structRegistry.build());

        // allocate a new response handler which shall take care of communicating operation results
        // to the client
        this.responseHandler = new NetworkResponseHandler(
                this,
                protocol().metadataHandler(),
                this.connector.configuration().streamingBufferSize(),
                this.connector.configuration().streamingFlushThreshold(),
                this.logService);

        // also enable any implicitly enabled features within the protocol version as we do not want these to be enabled
        // again if negotiated through one of the later mechanisms
        this.features.set(Collections.unmodifiableSet(protocol.features()));

        // allocate a new state machine for the desired protocol version to prepare the connection for handling requests
        var fsm = protocol.stateMachine().createInstance(this, this.logService);
        this.fsm = fsm;

        // notify the protocol in order to register legacy compliance listeners
        protocol.onConnectionNegotiated(this);

        // last notify any registered listeners to let them prepare the state machine if necessary
        this.notifyListeners(listener -> listener.onStateMachineInitialized(fsm));
    }

    private boolean enableFeature(Feature feature) {
        // ensure that the protocol has already been selected on this connection, otherwise we are incapable of enabling
        // features as the pipelines have yet to be initialized.
        if (this.protocol.get() == null) {
            throw new IllegalStateException("Connection has yet to select a protocol version");
        }

        // Ensure that we are the first and only thread to enable the desired feature on this connection - if the
        // feature is already enabled, this atomic swap will fail (or the set will already contain the selected
        // feature) thus preventing us from progressing further.
        Set<Feature> oldFeatures;
        Set<Feature> newFeatures = null;
        boolean enabled = false;
        do {
            oldFeatures = this.features.get();

            // Keep looping if the features list is still null - this means that there is likely a race condition
            // between selectProtocol and enableFeature thus preventing us from enabling features until the list of
            // implicitly enabled features of the protocol is known
            if (oldFeatures == null) {
                continue;
            }

            newFeatures = new HashSet<>(oldFeatures);
            enabled = newFeatures.add(feature);
        } while (oldFeatures == null
                || !this.features.compareAndSet(oldFeatures, Collections.unmodifiableSet(newFeatures)));

        if (!enabled) {
            // The feature has already been negotiated in some capacity meaning that whatever negotiation infrastructure
            // this call is originating from cannot succeed due to the state the connection is in (this likely means
            // that the feature was negotiated implicitly through the protocol and will thus not be enabled again).
            return false;
        }

        // Decorate the struct registry (e.g. replace it) in order to support reading of data types provided by the
        // selected feature. Since struct registries are immutable by design, we'll need to perform an atomic swap of
        // the registry object here (which may fail if multiple threads attempt to enable features at the same time). At
        // this point the only guarantee we get is that we are the only thread to enable the specified feature. Other
        // threads may still select a different feature at the same time.
        StructRegistry<Connection, Value> oldStructRegistry;
        StructRegistry<Connection, Value> decoratedStructRegistry = null;
        do {
            oldStructRegistry = this.structRegistry.get();

            // If the struct registry has yet to be initialized, we'll keep the loop alive as we cannot decorate a
            // null value - since we safeguard on the selected protocol, this condition should resolve immediately and
            // is likely a race condition between selectProtocol and enableFeature
            if (oldStructRegistry == null) {
                continue;
            }

            decoratedStructRegistry = feature.decorateStructRegistry(oldStructRegistry);
        } while (oldStructRegistry == null
                || !this.structRegistry.compareAndSet(oldStructRegistry, decoratedStructRegistry));

        // Extend the writer pipeline to include data types provided by the selected feature
        // Note: Writer pipelines are thread safe (albeit blocking) and thus do not require any additional checks within
        // this block.
        WriterPipeline pipeline;
        do {
            pipeline = this.writerPipeline;
        } while (pipeline == null);
        feature.configureWriterPipeline(pipeline);

        return true;
    }

    @Override
    public List<Feature> negotiate(
            List<Feature> features,
            String userAgent,
            RoutingContext routingContext,
            NotificationsConfig notificationsConfig,
            Map<String, String> boltAgent) {
        this.userAgent = userAgent;
        this.routingContext = routingContext;
        this.notificationsConfig = notificationsConfig;
        this.boltAgent = boltAgent;
        return features.stream().filter(this::enableFeature).toList();
    }

    @Override
    public PipelineContext writerContext(PackstreamBuf buf) {
        var pipeline = this.writerPipeline;
        if (pipeline == null) {
            throw new IllegalStateException("Connection has yet to select a protocol version");
        }

        return pipeline.forBuffer(buf);
    }

    @Override
    public PackstreamValueReader<Connection> valueReader(PackstreamBuf buf) {
        var structRegistry = this.structRegistry.get();
        if (structRegistry == null) {
            throw new IllegalStateException("Connection has yet to select a protocol version");
        }

        return new PackstreamValueReader<>(this, buf, structRegistry);
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
    public RoutingContext routingContext() {
        if (this.routingContext == null) {
            throw new IllegalStateException("Connection has yet to select routing context");
        }

        return this.routingContext;
    }

    @Override
    public AuthenticationFlag logon(Map<String, Object> token) throws AuthenticationException {
        this.connectionInfo =
                new BoltConnectionInfo(this.id, userAgent, this.clientAddress(), this.serverAddress(), this.boltAgent);

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

        this.notifyListeners(listener -> listener.onLogon(loginContext));

        if (result.credentialsExpired()) {
            return AuthenticationFlag.CREDENTIALS_EXPIRED;
        }
        return null;
    }

    @Override
    public void logoff() {
        if (!this.loginContext.compareAndSet(this.loginContext.get(), null)) {
            throw new IllegalStateException("Cannot logout as context is not what expected");
        }

        String username = this.username;
        this.username = null;

        this.notifyListeners(ConnectionListener::onLogoff);
        log.debug("[%s] Successfully logged off user %s and re-enabled throttles", this.id, username);
    }

    @Override
    public void impersonate(String userToImpersonate) throws AuthenticationException {
        Objects.requireNonNull(userToImpersonate, "userToImpersonate cannot be null");

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
    public void clearImpersonation() {
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

    @Override
    public Map<String, String> boltAgent() {
        return this.boltAgent;
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
                + connectionInfo + ", boltAgent="
                + boltAgent + ", username='"
                + username + '\'' + ", userAgent='"
                + userAgent + '\'' + ", defaultDatabase='"
                + defaultDatabase + '\'' + '}';
    }
}
