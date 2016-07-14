/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.state.membership;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.coreedge.raft.state.EndOfStreamException;
import org.neo4j.coreedge.raft.state.SafeStateMarshal;
import org.neo4j.coreedge.server.CoreMember;
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
    long ordinal; // persistence ordinal must be increased each time we change committed or appended

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

    public boolean append( long logIndex, Set<CoreMember> members )
    {
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

    public boolean uncommittedMemberChangeInLog()
    {
        return appended != null;
    }

    public Set<CoreMember> getLatest()
    {
        return appended != null ? appended.members() :
               committed != null ? committed.members() : new HashSet<>();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }
        RaftMembershipState that = (RaftMembershipState) o;
        return ordinal == that.ordinal &&
               Objects.equals( committed, that.committed ) &&
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
