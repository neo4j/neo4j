/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.discovery.procedures;

import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.QualifiedName;

public class InstalledProtocolsProcedure extends CallableProcedure.BasicProcedure
{
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};

    public static final String PROCEDURE_NAME = "protocols";

    private final Supplier<Stream<Pair<AdvertisedSocketAddress,ProtocolStack>>> clientInstalledProtocols;
    private final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols;

    public InstalledProtocolsProcedure( Supplier<Stream<Pair<AdvertisedSocketAddress,ProtocolStack>>> clientInstalledProtocols,
            Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> serverInstalledProtocols )
    {
        super( ProcedureSignature.procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( "orientation", Neo4jTypes.NTString )
                .out( "remoteAddress", Neo4jTypes.NTString )
                .out( "applicationProtocol", Neo4jTypes.NTString )
                .out( "version", Neo4jTypes.NTInteger )
                .description( "Overview of installed protocols" )
                .build() );
        this.clientInstalledProtocols = clientInstalledProtocols;
        this.serverInstalledProtocols = serverInstalledProtocols;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply(
            Context ctx, Object[] input, ResourceTracker resourceTracker ) throws ProcedureException
    {
        Stream<Object[]> outbound = toOutputRows( clientInstalledProtocols, ProtocolInstaller.Orientation.Client.OUTBOUND );

        Stream<Object[]> inbound = toOutputRows( serverInstalledProtocols, ProtocolInstaller.Orientation.Server.INBOUND );

        return Iterators.asRawIterator( Stream.concat( outbound, inbound ) );
    }

    private <T extends SocketAddress> Stream<Object[]> toOutputRows( Supplier<Stream<Pair<T,ProtocolStack>>> installedProtocols, String orientation )
    {
        return installedProtocols.get()
                .sorted( Comparator.comparing( entry -> entry.first().toString() ) )
                .map( entry -> new Object[]
                        {
                                orientation,
                                entry.first().toString(),
                                entry.other().applicationProtocol().identifier(),
                                (long) entry.other().applicationProtocol().version()
                        } );
    }
}
