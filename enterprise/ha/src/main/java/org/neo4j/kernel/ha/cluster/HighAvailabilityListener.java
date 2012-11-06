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

import java.net.URI;

/**
 * A HighAvailabilityListener is listening for events from elections and availability state.
 * <p/>
 * These are invoked by translating atomic broadcast messages to methods on this interface.
 */
public interface HighAvailabilityListener
{
    /**
     * Called when new master has been elected. The new master may not be available a.t.m.
     * A call to {@link #memberIsAvailable} will confirm that the master given in
     * the most recent {@link #masterIsElected(java.net.URI)} call is up and running as master.
     *
     * @param masterUri the connection information to the master.
     */
    void masterIsElected( URI masterUri );

    /**
     * Called when a member announces that it is available to play a particular role, e.g. master or slave.
     * After this it can be assumed that the member is ready to consume messages related to that role.
     *
     * @param role
     * @param instanceClusterUri
     * @param instanceUris
     */
    void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris );

    public abstract class Adapter
            implements HighAvailabilityListener
    {
        @Override
        public void masterIsElected( URI masterUri )
        {
        }

        @Override
        public void memberIsAvailable( String role, URI instanceClusterUri, Iterable<URI> instanceUris )
        {
        }
    }
}
