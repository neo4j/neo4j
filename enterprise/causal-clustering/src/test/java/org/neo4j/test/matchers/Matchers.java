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
package org.neo4j.test.matchers;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;

public final class Matchers
{
    private Matchers()
    {
    }

    public static Matcher<? super List<RaftMessages.RaftMessage>> hasMessage( RaftMessages.BaseRaftMessage message )
    {
        return new TypeSafeMatcher<List<RaftMessages.RaftMessage>>()
        {
            @Override
            protected boolean matchesSafely( List<RaftMessages.RaftMessage> messages )
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

    public static Matcher<? super List<RaftMessages.RaftMessage>> hasRaftLogEntries( Collection<RaftLogEntry>
            expectedEntries )
    {
        return new TypeSafeMatcher<List<RaftMessages.RaftMessage>>()
        {
            @Override
            protected boolean matchesSafely( List<RaftMessages.RaftMessage> messages )
            {
                List<RaftLogEntry> entries = messages.stream()
                        .filter( message -> message instanceof RaftMessages.AppendEntries.Request )
                        .map( m -> (RaftMessages.AppendEntries.Request) m )
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
