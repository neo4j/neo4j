/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.messaging;

import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;

public abstract class BoltResponseHandlerV1Adaptor implements BoltResponseHandler
{
    public void onRecords( BoltResult result, boolean pullAll ) throws Exception
    {
        if ( pullAll )
        {
            boolean hasMore = onPullRecords( result, PullAllMessage.PULL_N_SIZE );
            if ( hasMore )
            {
                throw new IllegalArgumentException( "Shall not allow pulling records multiple times in bolt v1" );
            }
        }
        else
        {
            onDiscardRecords( result );
        }
    }
}
