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
package org.neo4j.legacy.consistency.checking.full;

import org.neo4j.function.Predicate;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Stateful predicate that accepts record stream only once.
 * Used to make sure that node and relationship counts are computed only once during multi-pass consistency check.
 *
 * @param <T> type of the record.
 */
class MultiPassAvoidanceCondition<T extends AbstractBaseRecord> implements Predicate<T>
{
    private static final long NO_RECORDS_SEEN = -1;

    private long firstSeenRecordId = NO_RECORDS_SEEN;
    private boolean singlePassCompleted;

    @Override
    public boolean test( T record )
    {
        if ( singlePassCompleted )
        {
            return false;
        }
        if ( firstSeenRecordId == record.getLongId() )
        {
            singlePassCompleted = true;
            return false;
        }
        if ( firstSeenRecordId == NO_RECORDS_SEEN )
        {
            firstSeenRecordId = record.getLongId();
        }
        return true;
    }
}
