/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.PreparedIndexUpdates;

class AggregatedPreparedIndexUpdates implements PreparedIndexUpdates
{
    private final PreparedIndexUpdates[] aggregates;

    AggregatedPreparedIndexUpdates( PreparedIndexUpdates[] aggregates )
    {
        this.aggregates = aggregates;
    }

    @Override
    public void commit() throws IOException, IndexEntryConflictException
    {
        int lastCommittedIndex = -1;
        try
        {
            for ( int i = 0; i < aggregates.length; i++ )
            {
                aggregates[i].commit();
                lastCommittedIndex = i;
            }
        }
        finally
        {
            if ( lastCommittedIndex != aggregates.length - 1 )
            {
                for ( int i = lastCommittedIndex + 1; i < aggregates.length; i++ )
                {
                    aggregates[i].rollback();
                }
            }
        }
    }

    @Override
    public void rollback()
    {
        for ( PreparedIndexUpdates aggregate : aggregates )
        {
            aggregate.rollback();
        }
    }
}
