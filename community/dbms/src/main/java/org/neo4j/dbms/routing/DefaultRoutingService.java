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
package org.neo4j.dbms.routing;

import static org.neo4j.dbms.routing.RoutingTableServiceHelpers.FROM_ALIAS_KEY;
import static org.neo4j.kernel.api.exceptions.Status.Database.IllegalAliasChain;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.common.panic.PanicEventHandler;
import org.neo4j.common.panic.PanicReason;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.exceptions.Status.Routing;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public class DefaultRoutingService implements RoutingService, PanicEventHandler {
    private final InternalLog log;
    private final RoutingTableServiceValidator validator;
    private final ClientSideRoutingTableProvider clientSideRoutingTableProvider;
    private final ServerSideRoutingTableProvider serverSideRoutingTableProvider;
    private final ClientRoutingDomainChecker clientRoutingDomainChecker;
    private final Supplier<GraphDatabaseSettings.RoutingMode> defaultRouterSupplier;
    private final Supplier<Boolean> boltEnabled;
    private final InstanceClusterView instanceClusterView;
    private final DefaultDatabaseResolver defaultDatabaseResolver;

    private final DatabaseReferenceRepository databaseReferenceRepo;
    private final boolean echoRoutingContextAddressWhenAlone;
    private volatile PanicReason panicReason;

    public DefaultRoutingService(
            InternalLogProvider logProvider,
            RoutingTableServiceValidator validator,
            ClientSideRoutingTableProvider clientSideRoutingTableProvider,
            ServerSideRoutingTableProvider serverSideRoutingTableProvider,
            ClientRoutingDomainChecker clientRoutingDomainChecker,
            Config config,
            InstanceClusterView instanceClusterView,
            DefaultDatabaseResolver defaultDatabaseResolver,
            DatabaseReferenceRepository databaseReferenceRepo,
            boolean echoRoutingContextAddressWhenAlone) {
        this.log = logProvider.getLog(getClass());
        this.validator = validator;
        this.clientSideRoutingTableProvider = clientSideRoutingTableProvider;
        this.serverSideRoutingTableProvider = serverSideRoutingTableProvider;
        this.clientRoutingDomainChecker = clientRoutingDomainChecker;
        this.defaultRouterSupplier = () -> config.get(GraphDatabaseSettings.routing_default_router);
        this.boltEnabled = () -> config.get(BoltConnector.enabled);
        this.instanceClusterView = instanceClusterView;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.databaseReferenceRepo = databaseReferenceRepo;
        this.echoRoutingContextAddressWhenAlone = echoRoutingContextAddressWhenAlone;
    }

    @Override
    public RoutingResult route(String databaseName, String user, MapValue routingContext) throws RoutingException {
        assertNotInPanic();
        var databaseReference = extractDatabaseReference(databaseName, user);
        assertDatabaseExists(databaseReference);
        assertBoltConnectorEnabled(databaseReference);
        assertNotIllegalAliasChain(databaseReference, routingContext);
        return routeInternal(databaseReference, routingContext);
    }

    private void assertNotInPanic() throws RoutingException {
        if (panicReason != null) {
            throw new RoutingException(Routing.DbmsInPanic, panicReason.toString());
        }
    }

    private RoutingResult routeInternal(DatabaseReference databaseReference, MapValue routingContext)
            throws RoutingException {
        assertBoltConnectorEnabled(databaseReference);

        RoutingResult result;
        var clientProvidedAddress =
                RoutingTableServiceHelpers.findClientProvidedAddress(routingContext, BoltConnector.DEFAULT_PORT, log);
        var isInternalRef = databaseReference instanceof DatabaseReferenceImpl.Internal;
        if (!isInternalRef) {
            result = serverSideRoutingTableProvider.getServerSideRoutingTable(routingContext);
        } else {
            var defaultRouter = defaultRouterSupplier.get();
            if (configAllowsForClientSideRouting(defaultRouter, clientProvidedAddress)) {
                validator.isValidForClientSideRouting((DatabaseReferenceImpl.Internal) databaseReference);
                result = clientSideRoutingTableProvider.getRoutingResultForClientSideRouting(
                        (DatabaseReferenceImpl.Internal) databaseReference, routingContext);
            } else {
                validator.isValidForServerSideRouting((DatabaseReferenceImpl.Internal) databaseReference);
                result = serverSideRoutingTableProvider.getServerSideRoutingTable(routingContext);
            }
        }

        assertRoutingResultNotEmpty(result, databaseReference);
        return result;
    }

    private DatabaseReference extractDatabaseReference(String databaseName, String user) throws RoutingException {
        if (databaseName == null || databaseName.isEmpty()) {
            databaseName = defaultDatabaseResolver.defaultDatabase(user);
        }
        String finalDatabaseName = databaseName;
        return databaseReferenceRepo
                .getByAlias(databaseName)
                .orElseThrow(() -> RoutingTableServiceHelpers.databaseNotFoundException(finalDatabaseName));
    }

    private boolean configAllowsForClientSideRouting(
            GraphDatabaseSettings.RoutingMode defaultRouter, Optional<SocketAddress> clientProvidedAddress) {

        if (echoRoutingContextAddressWhenAlone && instanceClusterView.amIAlone()) {
            return false;
        }

        switch (defaultRouter) {
            case CLIENT:
                // in client mode everyone gets client routing behaviour all the time
                return true;
            case SERVER:
                // in server mode specific domains can be opted-in to client routing based on server configuration
                return clientProvidedAddress.isEmpty()
                        || clientRoutingDomainChecker.shouldGetClientRouting(clientProvidedAddress.get());
            default:
                throw new IllegalStateException("Unexpected value: " + defaultRouter);
        }
    }

    private void assertBoltConnectorEnabled(DatabaseReference databaseReference) throws RoutingException {
        if (!boltEnabled.get()) {
            throw new RoutingException(
                    ProcedureCallFailed,
                    "Cannot get routing table for " + databaseReference.alias()
                            + " because Bolt is not enabled. Please update your configuration for '"
                            + BoltConnector.enabled.name()
                            + "'");
        }
    }

    private static void assertRoutingResultNotEmpty(RoutingResult result, DatabaseReference databaseReference)
            throws RoutingException {
        if (result.containsNoEndpoints()) {
            throw new RoutingException(
                    DatabaseUnavailable, "Routing table for database " + databaseReference.alias() + " is empty");
        }
    }

    private void assertDatabaseExists(DatabaseReference databaseReference) throws RoutingException {
        databaseReferenceRepo
                .getByAlias(databaseReference.alias())
                .orElseThrow(() -> RoutingTableServiceHelpers.databaseNotFoundException(
                        databaseReference.alias().name()));
    }

    private void assertNotIllegalAliasChain(DatabaseReference databaseReference, MapValue routingContext)
            throws RoutingException {
        var refIsRemoteAlias = databaseReference instanceof DatabaseReferenceImpl.External;

        var sourceAlias = routingContext.get(FROM_ALIAS_KEY);
        var sourceAliasIsPresent = sourceAlias != null && sourceAlias != NO_VALUE;
        if (refIsRemoteAlias && sourceAliasIsPresent) {
            var sourceAliasString = ((TextValue) sourceAlias).stringValue();
            throw new RoutingException(
                    IllegalAliasChain,
                    "Unable to provide a routing table for the database '"
                            + databaseReference.alias().name() + "' because the request came from another alias '"
                            + sourceAliasString + "' and alias chains " + "are not permitted.");
        }
    }

    @Override
    public void onPanic(PanicReason reason, Throwable error) {
        this.panicReason = reason;
    }
}
