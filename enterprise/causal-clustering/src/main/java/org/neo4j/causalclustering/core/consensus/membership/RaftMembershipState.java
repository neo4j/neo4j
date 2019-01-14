/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.consensus.membership;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

/**
 * Represents the current state of membership in RAFT and exposes operations
 * for modifying the state. The valid states and transitions are represented
 * by the following table:
 *
 * state                           valid transitions
 * 1: [ -        , -        ]        2 (append)
 * 2: [ -        , appended ]      1,4 (commit or truncate)
 * 3: [ committed, appended ]        4 (commit or truncate)
 * 4: [ committed, -        ]        3 (append)
 *
 * The transition from 3->4 is either because the appended entry became
 * the new committed entry or because the appended entry was truncated.
 *
 * A committed entry can never be truncated and there can only be a single
 * outstanding appended entry which usually is committed shortly
 * thereafter, but it might also be truncated.
 *
 * Recovery must in-order replay all the log entries whose effects are not
 * guaranteed to have been persisted. The handling of these events is
 * idempotent so it is safe to replay entries which might have been
 * applied already.
 *
 * Note that commit updates occur separately from append/truncation in RAFT
 * so it is possible to for example observe several membership entries in a row
 * being appended on a particular member without an intermediate commit, even
 * though this is not possible in the system as a whole because the leader which
 * drives the membership change work will not spawn a new entry until it knows
 * that the previous one has been appended with a quorum, i.e. committed. This
 * is the reason why that this class is very lax when it comes to updating the
 * state and not making hard assertions which on a superficial level might
 * seem obvious. The consensus system as a whole and the membership change
 * driving logic is relied upon for achieving the correct system level
 * behaviour.
 */
public class RaftMembershipState extends LifecycleAdapter
{
    private MembershipEntry committed;
    private MembershipEntry appended;
    private long ordinal; // persistence ordinal must be increased each time we change committed or appended

    public RaftMembershipState()
    {
        this( -1, null, null );
    }

    RaftMembershipState( long ordinal, MembershipEntry committed, MembershipEntry appended )
    {
        this.ordinal = ordinal;
        this.committed = committed;
        this.appended = appended;
    }

    public boolean append( long logIndex, Set<MemberId> members )
    {
        if ( appended != null && logIndex <= appended.logIndex() )
        {
            return false;
        }

        if ( committed != null && logIndex <= committed.logIndex() )
        {
            return false;
        }

        if ( appended != null && (committed == null || appended.logIndex() > committed.logIndex()) )
        {
            /* This might seem counter-intuitive, but seeing two appended entries
            in a row must mean that the previous one got committed. So it must
            be recorded as having been committed or a subsequent truncation might
            erase the state. We also protect against going backwards in the
            committed state, as might happen during recovery. */

            committed = appended;
        }

        ordinal++;
        appended = new MembershipEntry( logIndex, members );
        return true;
    }

    public boolean truncate( long fromIndex )
    {
        if ( committed != null && fromIndex <= committed.logIndex() )
        {
            throw new IllegalStateException( "Truncating committed entry" );
        }

        if ( appended != null && fromIndex <= appended.logIndex() )
        {
            ordinal++;
            appended = null;
            return true;
        }
        return false;
    }

    public boolean commit( long commitIndex )
    {
        if ( appended != null && commitIndex >= appended.logIndex() )
        {
            ordinal++;
            committed = appended;
            appended = null;
            return true;
        }
        return false;
    }

    boolean uncommittedMemberChangeInLog()
    {
        return appended != null;
    }

    Set<MemberId> getLatest()
    {

        return appended != null ? appended.members() :
               committed != null ? committed.members() : new HashSet<>();
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
        RaftMembershipState that = (RaftMembershipState) o;
        return ordinal == that.ordinal && Objects.equals( committed, that.committed ) &&
                Objects.equals( appended, that.appended );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( committed, appended, ordinal );
    }

    @Override
    public String toString()
    {
        return "RaftMembershipState{" +
               "committed=" + committed +
               ", appended=" + appended +
               ", ordinal=" + ordinal +
               '}';
    }

    public RaftMembershipState newInstance()
    {
        return new RaftMembershipState( ordinal, committed, appended );
    }

    public MembershipEntry committed()
    {
        return committed;
    }

    public long getOrdinal()
    {
        return ordinal;
    }

    public static class Marshal extends SafeStateMarshal<RaftMembershipState>
    {
        MembershipEntry.Marshal entryMarshal = new MembershipEntry.Marshal();

        @Override
        public RaftMembershipState startState()
        {
            return new RaftMembershipState();
        }

        @Override
        public long ordinal( RaftMembershipState state )
        {
            return state.ordinal;
        }

        @Override
        public void marshal( RaftMembershipState state, WritableChannel channel ) throws IOException
        {
            channel.putLong( state.ordinal );
            entryMarshal.marshal( state.committed, channel );
            entryMarshal.marshal( state.appended, channel );
        }

        @Override
        public RaftMembershipState unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            long ordinal = channel.getLong();
            MembershipEntry committed = entryMarshal.unmarshal( channel );
            MembershipEntry appended = entryMarshal.unmarshal( channel );
            return new RaftMembershipState( ordinal, committed, appended );
        }
    }
}
