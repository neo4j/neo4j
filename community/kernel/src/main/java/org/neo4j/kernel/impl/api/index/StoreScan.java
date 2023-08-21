/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.index;

import org.neo4j.internal.kernel.api.PopulationProgress;

public interface StoreScan extends AutoCloseable {
    void run(ExternalUpdatesCheck externalUpdatesCheck);

    void stop();

    PopulationProgress getProgress();

    /**
     * Give this {@link StoreScan} a {@link PhaseTracker} to report to.
     * Must not be called once scan has already started.
     * @param phaseTracker {@link PhaseTracker} this store scan shall report to.
     */
    default void setPhaseTracker(PhaseTracker phaseTracker) { // no-op
    }

    @Override
    default void close() {}

    /**
     * Interaction point from the store scan with the index population to synchronize store scan with applying external concurrent updates
     * that happens while the store scan is running.
     */
    interface ExternalUpdatesCheck {
        /**
         * Called by the thread running the store scan from within the scan now and then to check whether or not there are external
         * updates to apply.
         */
        boolean needToApplyExternalUpdates();

        /**
         * Called after {@link #needToApplyExternalUpdates()} has returned {@code true} and preparations have been made so that external updates
         * can be applied w/o concurrent scan updates.
         * @param currentlyIndexedNodeId the highest entity id which has been processed by the store scan.
         */
        void applyExternalUpdates(long currentlyIndexedNodeId);
    }

    ExternalUpdatesCheck NO_EXTERNAL_UPDATES = new ExternalUpdatesCheck() {
        @Override
        public boolean needToApplyExternalUpdates() {
            return false;
        }

        @Override
        public void applyExternalUpdates(long currentlyIndexedNodeId) {}
    };
}
