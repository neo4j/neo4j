/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.function.Consumer;

/**
 * Abstract class that implement common logic for making the consumer to consume the {@link #description()} of this
 * threshold if {@link #thresholdReached(long)} is true.
 */
abstract class AbstractCheckPointThreshold implements CheckPointThreshold
{
    @Override
    public final boolean isCheckPointingNeeded( long lastCommittedTransactionId, Consumer<String> consumer )
    {
        boolean result = thresholdReached( lastCommittedTransactionId );
        try
        {
            return result;
        }
        finally
        {
            if ( result )
            {
                consumer.accept( description() );
            }
        }
    }

    protected abstract boolean thresholdReached( long lastCommittedTransactionId );

    protected abstract String description();
}
