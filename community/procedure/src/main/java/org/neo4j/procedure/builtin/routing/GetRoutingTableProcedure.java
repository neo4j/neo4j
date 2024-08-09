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
package org.neo4j.procedure.builtin.routing;

import static org.neo4j.dbms.routing.result.ParameterNames.CONTEXT;
import static org.neo4j.dbms.routing.result.ParameterNames.DATABASE;
import static org.neo4j.dbms.routing.result.ParameterNames.SERVERS;
import static org.neo4j.dbms.routing.result.ParameterNames.TTL;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.NO_VALUE;

import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.dbms.routing.RoutingException;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.dbms.routing.result.RoutingResultFormat;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

public final class GetRoutingTableProcedure implements CallableProcedure {
    public static final String[] DEFAULT_NAMESPACE = {"dbms", "routing"};
    public static final String[] LEGACY_NAMESPACE = {"dbms", "cluster", "routing"};

    private static final String NAME = "getRoutingTable";
    private static final String DESCRIPTION =
            "Returns the advertised bolt capable endpoints for a given database, divided by each endpoint's"
                    + " capabilities. For example, an endpoint may serve read queries, write queries, and/or future"
                    + " `getRoutingTable` requests.";

    private final RoutingService routingService;
    private final ProcedureSignature signature;
    private final InternalLog log;

    public record GetRoutingTableProcedures(GetRoutingTableProcedure old, GetRoutingTableProcedure current) {}

    public static GetRoutingTableProcedures from(RoutingService routingService, InternalLogProvider logProvider) {
        var currentSignature = createSignature(DEFAULT_NAMESPACE).build();
        var currentProcedureFullName = currentSignature.name().toString();
        var currentProcedure = new GetRoutingTableProcedure(routingService, currentSignature, logProvider);

        var oldSignatureBuilder = createSignature(LEGACY_NAMESPACE);
        var oldSignature = oldSignatureBuilder
                .isDeprecated(true)
                .deprecatedBy(currentProcedureFullName)
                .build();
        var oldProcedure = new GetRoutingTableProcedure(routingService, oldSignature, logProvider);

        return new GetRoutingTableProcedures(oldProcedure, currentProcedure);
    }

    public GetRoutingTableProcedure(
            RoutingService routingService, ProcedureSignature signature, InternalLogProvider logProvider) {
        this.routingService = routingService;
        this.signature = signature;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public ProcedureSignature signature() {
        return signature;
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {
        var user = ctx.securityContext().subject().executingUser();
        var databaseName = extractDatabaseName(input);
        var routingContext = extractRoutingContext(input);

        try {
            var result = routingService.route(databaseName, user, routingContext);
            log.info(
                    "Routing result for database %s and routing context %s is %s",
                    databaseName, routingContext, result);
            return ResourceRawIterator.<AnyValue[], ProcedureException>of(RoutingResultFormat.build(result));
        } catch (RoutingException ex) {
            throw new ProcedureException(ex.status(), ex, ex.getMessage());
        }
    }

    private String extractDatabaseName(AnyValue[] input) {
        var arg = input[1];
        if (arg == NO_VALUE) {
            return null;
        } else if (arg instanceof TextValue) {
            return ((TextValue) arg).stringValue();
        } else {
            throw new IllegalArgumentException("Illegal database name argument " + arg);
        }
    }

    private static MapValue extractRoutingContext(AnyValue[] input) {
        var arg = input[0];
        if (arg == NO_VALUE) {
            return MapValue.EMPTY;
        } else if (arg instanceof MapValue) {
            return (MapValue) arg;
        } else {
            throw new IllegalArgumentException("Illegal routing context argument " + arg);
        }
    }

    public static ProcedureSignature.Builder createSignature(String[] namespace) {
        return procedureSignature(new QualifiedName(namespace, NAME))
                .in(CONTEXT.parameterName(), Neo4jTypes.NTMap)
                .in(DATABASE.parameterName(), Neo4jTypes.NTString, nullValue(Neo4jTypes.NTString))
                .out(TTL.parameterName(), Neo4jTypes.NTInteger)
                .out(SERVERS.parameterName(), Neo4jTypes.NTList(Neo4jTypes.NTMap))
                .mode(Mode.DBMS)
                .description(GetRoutingTableProcedure.DESCRIPTION)
                .systemProcedure()
                .allowExpiredCredentials();
    }
}
