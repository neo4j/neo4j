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
