/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.time.Clock;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.check_point_policy;


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
     * <p>
     * This is important since we might have multiple thresholds or forced check points.
     *
     * @param transactionId the latest transaction committed id used by the check point
     */
    void checkPointHappened( long transactionId );

    /**
     * Return any desired checking frequencies, as a number of milliseconds between calls to
     * {@link #isCheckPointingNeeded(long, Consumer)}, if this {@link CheckPointThreshold} instance has any opinion on
     * the matter, or return {@link LongStream#empty()} is fine with some default checking frequency.
     * <p>
     * This is returned as an {@link LongStream} because a threshold might be composed of multiple other thresholds.
     * It is up to the caller to figure out how to best schedule this threshold, if the stream contains more than one
     * frequency. One way could be to use the lowest frequency, e.g. with {@link LongStream#min()}.
     *
     * @return A stream desired scheduling frequencies, if any specific ones are desired by this threshold.
     */
    LongStream checkFrequencyMillis();

    /**
     * Create and configure a {@link CheckPointThreshold} based on the given configurations.
     */
    static CheckPointThreshold createThreshold( Config config, Clock clock, LogProvider logProvider )
    {
        String policyName = config.get( check_point_policy );
        CheckPointThresholdPolicy policy;
        try
        {
            policy = CheckPointThresholdPolicy.loadPolicy( policyName );
        }
        catch ( NoSuchElementException e )
        {
            logProvider.getLog( CheckPointThreshold.class ).warn(
                    "Could not load check point policy '" + check_point_policy.name() + "=" + policyName + "'. " +
                    "Using default policy instead.", e );
            policy = new PeriodicThresholdPolicy();
        }
        return policy.createThreshold( config, clock, logProvider );
    }

    /**
     * Create a new {@link CheckPointThreshold} which will trigger if any of the given thresholds triggers.
     */
    static CheckPointThreshold or( final CheckPointThreshold... thresholds )
    {
        return new CheckPointThreshold()
        {
            @Override
            public void initialize( long transactionId )
            {
                for ( CheckPointThreshold threshold : thresholds )
                {
                    threshold.initialize( transactionId );
                }
            }

            @Override
            public boolean isCheckPointingNeeded( long transactionId, Consumer<String> consumer )
            {
                for ( CheckPointThreshold threshold : thresholds )
                {
                    if ( threshold.isCheckPointingNeeded( transactionId, consumer ) )
                    {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public void checkPointHappened( long transactionId )
            {
                for ( CheckPointThreshold threshold : thresholds )
                {
                    threshold.checkPointHappened( transactionId );
                }
            }

            @Override
            public LongStream checkFrequencyMillis()
            {
                return Stream.of( thresholds ).flatMapToLong( CheckPointThreshold::checkFrequencyMillis );
            }
        };
    }
}
