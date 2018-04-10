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
package org.neo4j.causalclustering.discovery;

import java.io.File;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;

public interface ClusterMember<T extends GraphDatabaseAPI>
{
    void start();

    void shutdown();

    boolean isShutdown();

    T database();

    ClientConnectorAddresses clientConnectorAddresses();

    String settingValue( String settingName );

    Config config();

    /**
     * {@link Cluster} will use this {@link ThreadGroup} for the threads that start, and shut down, this cluster member.
     * This way, the group will be transitively inherited by all the threads that are in turn started by the member
     * during its start up and shut down processes.
     * <p>
     * This helps with debugging, because it makes it immediately visible (in the debugger) which cluster member any
     * given thread belongs to.
     *
     * @return The intended parent thread group for this cluster member.
     */
    ThreadGroup threadGroup();

    Monitors monitors();

    File storeDir();

    File homeDir();

    default void updateConfig( Setting<?> setting, String value )
    {
        config().augment( setting, value );
    }
}
