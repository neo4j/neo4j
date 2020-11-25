/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration.connectors;

import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

/**
 * This is introduced to break the circular dependency between {@link GraphDatabaseSettings}
 * and {@link HttpConnector}, {@link HttpsConnector} & {@link BoltConnector}. This would cause classloading deadlocks if accessed out-of-order concurrently.
 * WARNING! None of these fields should be accessed outside the classes mentioned above.
 * Can be properly fixed in next major release (5.0) when the public API can be changed.
 */
@Deprecated( forRemoval = true )
public class ConnectorDefaults
{
    static final Setting<Boolean> http_enabled = newBuilder( "dbms.connector.http.enabled", BOOL, false ).build();
    static final Setting<Boolean> https_enabled = newBuilder( "dbms.connector.https.enabled", BOOL, false ).build();
    static final Setting<Boolean> bolt_enabled = newBuilder( "dbms.connector.bolt.enabled", BOOL, false ).build();

    public static final Map<Setting<?>,Object> SERVER_CONNECTOR_DEFAULTS = Map.of(
            http_enabled, true,
            https_enabled, false,
            bolt_enabled, true
    );
}
