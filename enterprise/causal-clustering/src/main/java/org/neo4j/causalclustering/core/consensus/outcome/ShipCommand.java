/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.outcome;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper;

import static java.lang.String.format;

public abstract class ShipCommand
{
    public abstract void applyTo( RaftLogShipper raftLogShipper, LeaderContext leaderContext );

    public static class Mismatch extends ShipCommand
    {
        private final long lastRemoteAppendIndex;
        private final Object target;

        public Mismatch( long lastRemoteAppendIndex, Object target )
        {
            this.lastRemoteAppendIndex = lastRemoteAppendIndex;
            this.target = target;
        }

        @Override
        public void applyTo( RaftLogShipper raftLogShipper, LeaderContext leaderContext )
        {
            if ( raftLogShipper.identity().equals( target ) )
            {
                raftLogShipper.onMismatch( lastRemoteAppendIndex, leaderContext );
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Mismatch mismatch = (Mismatch) o;

            if ( lastRemoteAppendIndex != mismatch.lastRemoteAppendIndex )
            {
                return false;
            }
            return target.equals( mismatch.target );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (lastRemoteAppendIndex ^ (lastRemoteAppendIndex >>> 32));
            result = 31 * result + target.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Mismatch{lastRemoteAppendIndex=%d, target=%s}", lastRemoteAppendIndex, target );
        }
    }

    public static class Match extends ShipCommand
    {
        private final long newMatchIndex;
        private final Object target;

        public Match( long newMatchIndex, Object target )
        {
            this.newMatchIndex = newMatchIndex;
            this.target = target;
        }

        @Override
        public  void applyTo( RaftLogShipper raftLogShipper, LeaderContext leaderContext )
        {
            if ( raftLogShipper.identity().equals( target ) )
            {
                raftLogShipper.onMatch( newMatchIndex, leaderContext );
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            Match match = (Match) o;

            if ( newMatchIndex != match.newMatchIndex )
            {
                return false;
            }
            return target.equals( match.target );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (newMatchIndex ^ (newMatchIndex >>> 32));
            result = 31 * result + target.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Match{newMatchIndex=%d, target=%s}", newMatchIndex, target );
        }
    }

    public static class NewEntries extends ShipCommand
    {
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final RaftLogEntry[] newLogEntries;

        public NewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newLogEntries )
        {
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.newLogEntries = newLogEntries;
        }

        @Override
        public  void applyTo( RaftLogShipper raftLogShipper, LeaderContext leaderContext )
        {
            raftLogShipper.onNewEntries( prevLogIndex, prevLogTerm, newLogEntries, leaderContext );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            NewEntries newEntries = (NewEntries) o;

            if ( prevLogIndex != newEntries.prevLogIndex )
            {
                return false;
            }
            if ( prevLogTerm != newEntries.prevLogTerm )
            {
                return false;
            }
            return Arrays.equals( newLogEntries, newEntries.newLogEntries );

        }

        @Override
        public int hashCode()
        {
            int result = (int) (prevLogIndex ^ (prevLogIndex >>> 32));
            result = 31 * result + (int) (prevLogTerm ^ (prevLogTerm >>> 32));
            result = 31 * result + Arrays.hashCode( newLogEntries );
            return result;
        }

        @Override
        public String toString()
        {
            return format( "NewEntry{prevLogIndex=%d, prevLogTerm=%d, newLogEntry=%s}", prevLogIndex, prevLogTerm,
                    Arrays.toString( newLogEntries ) );
        }
    }

    public static class CommitUpdate extends ShipCommand
    {
        @Override
        public  void applyTo( RaftLogShipper raftLogShipper, LeaderContext leaderContext )
        {
            raftLogShipper.onCommitUpdate( leaderContext );
        }

        @Override
        public String toString()
        {
            return "CommitUpdate{}";
        }
    }
}
