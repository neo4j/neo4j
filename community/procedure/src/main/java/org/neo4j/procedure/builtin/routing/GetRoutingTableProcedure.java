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
package org.neo4j.procedure.builtin.routing;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.procedure.Mode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.procedure.builtin.routing.ParameterNames.CONTEXT;
import static org.neo4j.procedure.builtin.routing.ParameterNames.DATABASE;
import static org.neo4j.procedure.builtin.routing.ParameterNames.SERVERS;
import static org.neo4j.procedure.builtin.routing.ParameterNames.TTL;

public final class GetRoutingTableProcedure implements CallableProcedure
{
    private static final String NAME = "getRoutingTable";
    public static final String ADDRESS_CONTEXT_KEY = "address";

    private final ProcedureSignature signature;
    private final DatabaseManager<?> databaseManager;

    protected final InternalLog log;
    private final RoutingTableProcedureValidator validator;
    private final ClientSideRoutingTableProvider clientSideRoutingTableProvider;
    private final ServerSideRoutingTableProvider serverSideRoutingTableProvider;
    private final ClientRoutingDomainChecker clientRoutingDomainChecker;
    private final String defaultDatabaseName;
    private final Supplier<GraphDatabaseSettings.RoutingMode> defaultRouterSupplier;
    private final Supplier<Boolean> boltEnabled;

    public GetRoutingTableProcedure( List<String> namespace, String description, DatabaseManager<?> databaseManager,
                                     RoutingTableProcedureValidator validator, SingleAddressRoutingTableProvider routingTableProvider,
                                     ClientRoutingDomainChecker clientRoutingDomainChecker, Config config, InternalLogProvider logProvider )
    {
        this( namespace, description, databaseManager, validator, routingTableProvider, routingTableProvider, clientRoutingDomainChecker, config, logProvider );
    }

    public GetRoutingTableProcedure( List<String> namespace, String description, DatabaseManager<?> databaseManager,
                                     RoutingTableProcedureValidator validator, ClientSideRoutingTableProvider clientSideRoutingTableProvider,
                                     ServerSideRoutingTableProvider serverSideRoutingTableProvider, ClientRoutingDomainChecker clientRoutingDomainChecker,
                                     Config config, InternalLogProvider logProvider )
    {
        this.signature = buildSignature( namespace, description );
        this.databaseManager = databaseManager;
        this.log = logProvider.getLog( getClass() );
        this.validator = validator;
        this.clientSideRoutingTableProvider = clientSideRoutingTableProvider;
        this.serverSideRoutingTableProvider = serverSideRoutingTableProvider;
        this.clientRoutingDomainChecker = clientRoutingDomainChecker;
        this.defaultDatabaseName = config.get( default_database );
        this.defaultRouterSupplier = () -> config.get( GraphDatabaseSettings.routing_default_router );
        this.boltEnabled = () -> config.get( BoltConnector.enabled );
    }

    @Override
    public ProcedureSignature signature()
    {
        return signature;
    }

    @Override
    public RawIterator<AnyValue[],ProcedureException> apply( Context ctx, AnyValue[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        var databaseId = extractDatabaseId( input );
        var routingContext = extractRoutingContext( input );

        assertBoltConnectorEnabled( databaseId );
        try
        {
            var result = invoke( databaseId, routingContext );
            log.debug( "Routing result for database %s and routing context %s is %s", databaseId, routingContext, result );
            assertRoutingResultNotEmpty( result, databaseId );
            return RawIterator.<AnyValue[],ProcedureException>of( RoutingResultFormat.build( result ) );
        }
        catch ( ProcedureException ex )
        {
            // Check that the cause of the exception wasn't the database being removed while this procedure was running.
            validator.assertDatabaseExists( databaseId );
            // otherwise re-throw
            throw ex;
        }
    }

    private RoutingResult invoke( NamedDatabaseId databaseId, MapValue routingContext ) throws ProcedureException
    {
        var clientProvidedAddress = RoutingTableProcedureHelpers.findClientProvidedAddress( routingContext, BoltConnector.DEFAULT_PORT, log );
        var defaultRouter = defaultRouterSupplier.get();
        if ( shouldGetClientSideRouting( defaultRouter, clientProvidedAddress ) )
        {
            validator.isValidForClientSideRouting( databaseId );
            return clientSideRoutingTableProvider.getRoutingResultForClientSideRouting( databaseId, routingContext );
        }
        else
        {
            validator.isValidForServerSideRouting( databaseId );
            return serverSideRoutingTableProvider.getServerSideRoutingTable( clientProvidedAddress );
        }
    }

    private NamedDatabaseId extractDatabaseId( AnyValue[] input ) throws ProcedureException
    {
        var arg = input[1];
        final String databaseName;
        if ( arg == Values.NO_VALUE )
        {
            databaseName = defaultDatabaseName;
        }
        else if ( arg instanceof TextValue )
        {
            databaseName = ((TextValue) arg).stringValue();
        }
        else
        {
            throw new IllegalArgumentException( "Illegal database name argument " + arg );
        }
        var databaseId = databaseManager.databaseIdRepository()
                                        .getByName( databaseName )
                                        .orElseThrow( () -> RoutingTableProcedureHelpers.databaseNotFoundException( databaseName ) );
        return databaseId;
    }

    private static void assertRoutingResultNotEmpty( RoutingResult result, NamedDatabaseId namedDatabaseId ) throws ProcedureException
    {
        if ( result.containsNoEndpoints() )
        {
            throw new ProcedureException( DatabaseUnavailable, "Routing table for database " + namedDatabaseId.name() + " is empty" );
        }
    }

    private static MapValue extractRoutingContext( AnyValue[] input )
    {
        var arg = input[0];
        if ( arg == Values.NO_VALUE )
        {
            return MapValue.EMPTY;
        }
        else if ( arg instanceof MapValue )
        {
            return (MapValue) arg;
        }
        else
        {
            throw new IllegalArgumentException( "Illegal routing context argument " + arg );
        }
    }

    private static ProcedureSignature buildSignature( List<String> namespace, String description )
    {
        return procedureSignature( new QualifiedName( namespace, NAME ) )
                .in( CONTEXT.parameterName(), Neo4jTypes.NTMap )
                .in( DATABASE.parameterName(), Neo4jTypes.NTString, nullValue( Neo4jTypes.NTString ) )
                .out( TTL.parameterName(), Neo4jTypes.NTInteger )
                .out( SERVERS.parameterName(), Neo4jTypes.NTList( Neo4jTypes.NTMap ) )
                .mode( Mode.DBMS )
                .description( description )
                .systemProcedure()
                .allowExpiredCredentials()
                .build();
    }

    private boolean shouldGetClientSideRouting( GraphDatabaseSettings.RoutingMode defaultRouter, Optional<SocketAddress> clientProvidedAddress )
    {
        switch ( defaultRouter )
        {
        case CLIENT:
            // in client mode everyone gets client routing behaviour all the time
            return true;
        case SERVER:
            // in server mode specific domains can be opted-in to client routing based on server configuration
            return clientProvidedAddress.isEmpty() || clientRoutingDomainChecker.shouldGetClientRouting( clientProvidedAddress.get() );
        default:
            throw new IllegalStateException( "Unexpected value: " + defaultRouter );
        }
    }

    private void assertBoltConnectorEnabled( NamedDatabaseId namedDatabaseId ) throws ProcedureException
    {
        if ( !boltEnabled.get() )
        {
            throw new ProcedureException( ProcedureCallFailed, "Cannot get routing table for " + namedDatabaseId.name() +
                                                               " because Bolt is not enabled. Please update your configuration for '" +
                                                               BoltConnector.enabled.name() + "'" );
        }
    }
}
