/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.consistency.checking.old;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

@Deprecated
public class MonitoringConsistencyReporter implements InconsistencyReport
{
    private final ConsistencyReporter reporter;
    private int inconsistencyCount = 0;

    public MonitoringConsistencyReporter( ConsistencyReporter reporter )
    {
        this.reporter = reporter;
    }

    // Inconsistency between two records
    public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
            InconsistencyType type )
    {
        reporter.report( recordStore, record, referredStore, referred, type );
        if ( type.isWarning() )
        {
            return false;
        }
        else
        {
            inconsistencyCount++;
            return true;
        }
    }

    public <R extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R> store, R record, R referred, InconsistencyType type )
    {
        reporter.report( store, record, store, referred, type );
        if ( type.isWarning() )
        {
            return false;
        }
        else
        {
            inconsistencyCount++;
            return true;
        }
    }

    // Internal inconsistency in a single record
    public <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record, InconsistencyType type )
    {
        reporter.report( store, record, type );
        if ( type.isWarning() )
        {
            return false;
        }
        else
        {
            inconsistencyCount++;
            return true;
        }
    }

    /**
     * Check if any inconsistencies was found by the checker. This method should
     * be invoked at the end of the check. If inconsistencies were found an
     * {@link AssertionError} summarizing the number of inconsistencies will be
     * thrown.
     *
     * @throws AssertionError if any inconsistencies were found.
     */
    public void checkResult() throws AssertionError
    {
        if ( inconsistencyCount != 0 )
        {
            throw new AssertionError(
                    String.format( "Store level inconsistency found in %d places", inconsistencyCount ) );
        }
    }
}
