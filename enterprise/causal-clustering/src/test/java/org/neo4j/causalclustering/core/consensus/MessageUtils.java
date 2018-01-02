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
package org.neo4j.causalclustering.core.consensus;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.helpers.collection.Iterables;

import static java.lang.String.format;

public class MessageUtils
{
    private MessageUtils()
    {
    }

    public static RaftMessages.RaftMessage messageFor( Outcome outcome, final MemberId member )
    {
        Predicate<RaftMessages.Directed> selectMember = message -> message.to() == member;
        try
        {
            return Iterables.single( new FilteringIterable<>( outcome.getOutgoingMessages(), selectMember ) )
                        .message();
        }
        catch ( NoSuchElementException e )
        {
            throw new AssertionError( format( "Expected message for %s, but outcome only contains %s.",
                    member, outcome.getOutgoingMessages() ) );
        }
    }
}
