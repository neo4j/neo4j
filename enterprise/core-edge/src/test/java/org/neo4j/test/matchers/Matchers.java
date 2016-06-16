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
package org.neo4j.test.matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.server.RaftTestMember;

public final class Matchers
{
    private Matchers()
    {
    }

    public static <MEMBER> Matcher<? super List<RaftMessages.RaftMessage<MEMBER>>> hasMessage( RaftMessages.BaseMessage<MEMBER> message )
    {
        return new TypeSafeMatcher<List<RaftMessages.RaftMessage<MEMBER>>>()
        {
            @Override
            protected boolean matchesSafely( List<RaftMessages.RaftMessage<MEMBER>> messages )
            {
                return messages.contains( message );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "has message " + message );
            }
        };
    }

    public static <MEMBER> Matcher<? super List<RaftMessages.RaftMessage<MEMBER>>> hasRaftLogEntries( Collection<RaftLogEntry> expectedEntries )
    {
        return new TypeSafeMatcher<List<RaftMessages.RaftMessage<MEMBER>>>()
        {
            @Override
            protected boolean matchesSafely( List<RaftMessages.RaftMessage<MEMBER>> messages )
            {
                List<RaftLogEntry> entries = messages.stream()
                        .filter( message -> message instanceof RaftMessages.AppendEntries.Request )
                        .map( m -> ((RaftMessages.AppendEntries.Request) m) )
                        .flatMap( x -> Arrays.stream( x.entries() ) )
                        .collect( Collectors.toList() );

                return entries.containsAll( expectedEntries );

            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "log entries " + expectedEntries );
            }
        };
    }
}
