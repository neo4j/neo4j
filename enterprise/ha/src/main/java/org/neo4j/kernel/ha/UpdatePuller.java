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
package org.neo4j.kernel.ha;


/**
 * Puller of transactions updates from a different store. Pulls for updates and applies them into a current store.
 * <p>
 * On a running instance of a store there should be only one active implementation of this interface.
 * <p>
 * Typically master instance should use {@link #NONE} implementation since master is owner of data in cluster env
 * and its up to slaves to pull updates.
 *
 * @see SlaveUpdatePuller
 */
public interface UpdatePuller
{
    /**
     * Pull all available updates.
     *
     * @throws InterruptedException in case if interrupted while waiting for updates
     */
    void pullUpdates() throws InterruptedException;


    /**
     * Try to pull all updates
     *
     * @return true if all updates pulled, false if updater fail on update retrieval
     * @throws InterruptedException in case if interrupted while waiting for updates
     */
    boolean tryPullUpdates() throws InterruptedException;

    /**
     * Pull updates and waits for the supplied condition to be
     * fulfilled as part of the update pulling happening.
     *
     * @param condition {@link UpdatePuller.Condition} to wait for.
     * @param assertPullerActive if {@code true} then observing an inactive update puller
     * will throw an {@link IllegalStateException},
     * @throws InterruptedException if we were interrupted while awaiting the condition.
     * @throws IllegalStateException if {@code strictlyAssertActive} and the update puller
     * became inactive while awaiting the condition.
     */
    void pullUpdates( Condition condition, boolean assertPullerActive ) throws InterruptedException;

    /**
     * Condition to be meet during update pulling.
     */
    interface Condition
    {
        boolean evaluate( int currentTicket, int targetTicket );
    }

    UpdatePuller NONE = new UpdatePuller()
    {
        @Override
        public void pullUpdates() throws InterruptedException
        {
        }

        @Override
        public boolean tryPullUpdates() throws InterruptedException
        {
            return false;
        }

        @Override
        public void pullUpdates( Condition condition, boolean assertPullerActive )
                throws InterruptedException
        {

        }
    };
}
