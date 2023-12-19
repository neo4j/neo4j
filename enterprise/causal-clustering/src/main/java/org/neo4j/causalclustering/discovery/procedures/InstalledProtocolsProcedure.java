/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.discovery.procedures;

import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;

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
                .out( "applicationProtocolVersion", Neo4jTypes.NTInteger )
                .out( "modifierProtocols", Neo4jTypes.NTString )
                .description( "Overview of installed protocols" )
                .build() );
        this.clientInstalledProtocols = clientInstalledProtocols;
        this.serverInstalledProtocols = serverInstalledProtocols;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply(
            Context ctx, Object[] input, ResourceTracker resourceTracker )
    {
        Stream<Object[]> outbound = toOutputRows( clientInstalledProtocols, ProtocolInstaller.Orientation.Client.OUTBOUND );

        Stream<Object[]> inbound = toOutputRows( serverInstalledProtocols, ProtocolInstaller.Orientation.Server.INBOUND );

        return Iterators.asRawIterator( Stream.concat( outbound, inbound ) );
    }

    private <T extends SocketAddress> Stream<Object[]> toOutputRows( Supplier<Stream<Pair<T,ProtocolStack>>> installedProtocols, String orientation )
    {
        Comparator<Pair<T,ProtocolStack>> connectionInfoComparator = Comparator.comparing( ( Pair<T,ProtocolStack> entry ) -> entry.first().getHostname() )
                .thenComparing( entry -> entry.first().getPort() );

        return installedProtocols.get()
                .sorted( connectionInfoComparator )
                .map( entry -> buildRow( entry, orientation ) );
    }

    private <T extends SocketAddress> Object[] buildRow( Pair<T,ProtocolStack> connectionInfo, String orientation )
    {
        T socketAddress = connectionInfo.first();
        ProtocolStack protocolStack = connectionInfo.other();
        return new Object[]
                {
                    orientation,
                    socketAddress.toString(),
                    protocolStack.applicationProtocol().category(),
                    (long) protocolStack.applicationProtocol().implementation(),
                    modifierString( protocolStack )
                };
    }

    private String modifierString( ProtocolStack protocolStack )
    {
        return protocolStack
                .modifierProtocols()
                .stream()
                .map( Protocol.ModifierProtocol::implementation )
                .collect( Collectors.joining( ",", "[", "]") );
    }
}
