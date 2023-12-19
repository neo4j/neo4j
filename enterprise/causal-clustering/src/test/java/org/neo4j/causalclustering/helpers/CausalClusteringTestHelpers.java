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
package org.neo4j.causalclustering.helpers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CausalClusteringTestHelpers
{
    public static String transactionAddress( GraphDatabaseFacade graphDatabase )
    {
        AdvertisedSocketAddress hostnamePort = graphDatabase
                .getDependencyResolver()
                .resolveDependency( Config.class )
                .get( CausalClusteringSettings.transaction_advertised_address );
        return String.format( "%s:%s", hostnamePort.getHostname(), hostnamePort.getPort() );
    }

    public static String backupAddress( GraphDatabaseFacade graphDatabaseFacade )
    {
        HostnamePort backupAddress = graphDatabaseFacade
                .getDependencyResolver()
                .resolveDependency( Config.class )
                .get( OnlineBackupSettings.online_backup_server );
        return String.format( "%s:%s", backupAddress.getHost(), backupAddress.getPort() );
    }

    public static Map<Integer, String> distributeDatabaseNamesToHostNums( int nHosts, Set<String> databaseNames )
    {
        //Max number of hosts per database is (nHosts / nDatabases) or (nHosts / nDatabases) + 1
        int nDatabases = databaseNames.size();
        int maxCapacity = (nHosts % nDatabases == 0) ? (nHosts / nDatabases) : (nHosts / nDatabases) + 1;

        List<String> repeated = databaseNames.stream()
                .flatMap( db -> IntStream.range( 0, maxCapacity ).mapToObj( ignored -> db ) )
                .collect( Collectors.toList() );

        Map<Integer,String> mapping = new HashMap<>( nHosts );

        for ( int hostId = 0; hostId < nHosts; hostId++ )
        {
            mapping.put( hostId, repeated.get( hostId ) );
        }
        return mapping;
    }
}
