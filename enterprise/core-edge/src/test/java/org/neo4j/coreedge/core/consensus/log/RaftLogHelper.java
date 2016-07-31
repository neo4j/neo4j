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
package org.neo4j.coreedge.core.consensus.log;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;

public class RaftLogHelper
{
    public static RaftLogEntry readLogEntry( ReadableRaftLog raftLog, long index ) throws IOException
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( index ) )
        {
            if ( cursor.next() )
            {
                return cursor.get();
            }
        }

        //todo: do not do this and update RaftLogContractTest to not depend on this exception.
        throw new IOException("Asked for raft log entry at index " + index + " but it was not found");
    }

    public static Matcher<? super RaftLog> hasNoContent( long index )
    {
        return new TypeSafeMatcher<RaftLog>()
        {
            @Override
            protected boolean matchesSafely( RaftLog log )
            {
                try
                {
                    readLogEntry( log, index );
                }
                catch ( IOException e )
                {
                    // oh well...
                    return true;
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Log should not contain entry at index " ).appendValue( index );
            }
        };
    }
}
