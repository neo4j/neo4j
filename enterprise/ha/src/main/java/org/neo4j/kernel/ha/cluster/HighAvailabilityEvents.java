/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

/**
 * Sits closest to the distributed master election algorithm and can provide input to
 * it and also delegate events to listeners.
 * <p/>
 * (DISTRIBUTED MASTER ELECTION THINGIE) ----- (MasterElectionClient for db 1) ----- (listener 1)
 * |                                                    |----- (listener 2)
 * |
 * (MasterElectionClient for db 2)
 * |
 * |
 * (listener 3)
 */
public interface HighAvailabilityEvents
{
    /**
     * When a member has finished a transition to a particular role, i.e. master or slave,
     * then it should call this which will broadcast the new status to the cluster.
     *
     * @param role
     */
    void memberIsAvailable( String role );

    void addClusterEventListener( HighAvailabilityListener listener );

    void removeClusterEventListener( HighAvailabilityListener listener );
}
