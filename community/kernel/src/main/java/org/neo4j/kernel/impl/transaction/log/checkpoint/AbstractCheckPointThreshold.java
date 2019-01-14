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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.util.function.Consumer;

/**
 * Abstract class that implement common logic for making the consumer to consume the description of this
 * threshold if {@link #thresholdReached(long)} is true.
 */
public abstract class AbstractCheckPointThreshold implements CheckPointThreshold
{
    private final String description;

    public AbstractCheckPointThreshold( String description )
    {
        this.description = description;
    }

    @Override
    public final boolean isCheckPointingNeeded( long lastCommittedTransactionId, Consumer<String> consumer )
    {
        if ( thresholdReached( lastCommittedTransactionId ) )
        {
            consumer.accept( description );
            return true;
        }
        return false;
    }

    protected abstract boolean thresholdReached( long lastCommittedTransactionId );
}
