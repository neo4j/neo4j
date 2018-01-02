/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.function.Consumer;

/**
 * A check point threshold provides information if a check point is required or not.
 */
public interface CheckPointThreshold
{
    /**
     * This method initialize the threshold by providing the initial transaction id
     *
     * @param transactionId the latest transaction committed id
     */
    void initialize( long transactionId );

    /**
     * This method can be used for querying the threshold about the necessity of a check point.
     *
     * @param lastCommittedTransactionId the latest transaction committed id
     * @param consumer will be called with the description about this threshold only if the return value is true
     * @return true is a check point is needed, false otherwise.
     */
    boolean isCheckPointingNeeded( long lastCommittedTransactionId, Consumer<String> consumer );

    /**
     * This method notifies the threshold that a check point has happened. This must be called every time a check point
     * has been written in the transaction log in order to make sure that the threshold updates its condition.
     *
     * This is important since we might have multiple thresholds or forced check points.
     *
     * @param transactionId the latest transaction committed id used by the check point
     */
    void checkPointHappened( long transactionId );
}
