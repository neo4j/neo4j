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
package org.neo4j.causalclustering.core.consensus.log;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;

public class RaftLogHelper
{
    private RaftLogHelper()
    {
    }

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
        throw new IOException( "Asked for raft log entry at index " + index + " but it was not found" );
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
