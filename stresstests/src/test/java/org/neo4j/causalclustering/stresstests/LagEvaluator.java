/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.stresstests;

import java.time.Clock;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.multiplyExact;
import static java.lang.String.format;

class LagEvaluator
{
    class Lag
    {
        private final long timeLagMillis;
        private final long valueLag;

        Lag( long timeLagMillis, long valueLag )
        {
            this.timeLagMillis = timeLagMillis;
            this.valueLag = valueLag;
        }

        long timeLagMillis()
        {
            return timeLagMillis;
        }

        @Override
        public String toString()
        {
            return "Lag{" + "timeLagMillis=" + timeLagMillis + ", valueLag=" + valueLag + '}';
        }
    }

    private final Supplier<OptionalLong> leader;
    private final Supplier<OptionalLong> follower;
    private final Clock clock;

    private Sample previous = Sample.INCOMPLETE;

    LagEvaluator( Supplier<OptionalLong> leader, Supplier<OptionalLong> follower, Clock clock )
    {
        this.leader = leader;
        this.follower = follower;
        this.clock = clock;
    }

    Optional<Lag> evaluate()
    {
        Sample current = sampleNow();
        Optional<Lag> lag = estimateLag( previous, current );
        previous = current;
        return lag;
    }

    private Optional<Lag> estimateLag( Sample previous, Sample current )
    {
        if ( previous.incomplete() || current.incomplete() )
        {
            return Optional.empty();
        }

        if ( current.timeStampMillis <= previous.timeStampMillis )
        {
            throw new RuntimeException( format( "Time not progressing: %s -> %s", previous, current ) );
        }
        else if ( current.follower < previous.follower )
        {
            throw new RuntimeException( format( "Follower going backwards: %s -> %s", previous, current ) );
        }
        else if ( current.follower == previous.follower )
        {
            return Optional.empty();
        }

        long valueLag = current.leader - current.follower;
        long dTime = current.timeStampMillis - previous.timeStampMillis;
        long dFollower = current.follower - previous.follower;
        long timeLagMillis = floorDiv( multiplyExact( valueLag, dTime ), dFollower );

        return Optional.of( new Lag( timeLagMillis, valueLag ) );
    }

    private Sample sampleNow()
    {
        // sample follower before leader, to avoid erroneously observing the follower as being ahead
        OptionalLong followerSample = follower.get();
        OptionalLong leaderSample = leader.get();

        if ( !followerSample.isPresent() || !leaderSample.isPresent() )
        {
            return Sample.INCOMPLETE;
        }

        return new Sample( leaderSample.getAsLong(), followerSample.getAsLong(), clock.millis() );
    }

    private static class Sample
    {
        private static final Sample INCOMPLETE = new Sample()
        {
            @Override
            boolean incomplete()
            {
                return true;
            }
        };

        private final long timeStampMillis;
        private final long leader;
        private final long follower;

        Sample()
        {
            timeStampMillis = 0;
            follower = 0;
            leader = 0;
        }

        Sample( long leader, long follower, long timeStampMillis )
        {
            this.timeStampMillis = timeStampMillis;
            this.leader = leader;
            this.follower = follower;
        }

        boolean incomplete()
        {
            return false;
        }
    }
}
