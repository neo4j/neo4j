/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.test.coreedge;

import org.neo4j.coreedge.discovery.DiscoveryServiceFactory;
import org.neo4j.graphdb.config.Setting;

import java.util.Map;
import java.util.function.IntFunction;

public interface ClusterBuilder<C extends ClusterBuilder>
{
    C withNumberOfCoreServers( int noCoreServers );

    C withNumberOfEdgeServers( int noEdgeServers );

    C withDiscoveryServiceFactory( DiscoveryServiceFactory factory );

    C withSharedCoreParams( Map<String,String> params );

    C withSharedCoreParam( Setting<?> key, String value );

    C withInstanceCoreParams( Map<String,IntFunction<String>> params );

    C withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction );

    C withSharedEdgeParams( Map<String,String> params );

    C withSharedEdgeParam( Setting<?> key, String value );

    C withInstanceEdgeParams( Map<String,IntFunction<String>> params );

    C withInstanceEdgeParam( Setting<?> key, IntFunction<String> valueFunction );

    C withRecordFormat( String recordFormat );
}
