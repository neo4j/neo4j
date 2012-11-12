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
package org.neo4j.consistency.report;

import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;

public class PendingReferenceCheck<REFERENCED extends AbstractBaseRecord>
{
    private ConsistencyReport report;
    private final ComparativeRecordChecker checker;

    PendingReferenceCheck( ConsistencyReport report, ComparativeRecordChecker checker )
    {
        this.report = report;
        this.checker = checker;
    }

    @Override
    public synchronized String toString()
    {
        if (report == null)
        {
            return String.format( "CompletedReferenceCheck{%s}", checker );
        } else {
            return ConsistencyReporter.pendingCheckToString(report, checker);
        }
    }

    public void checkReference( REFERENCED referenced, RecordAccess records )
    {
        ConsistencyReporter.dispatchReference( report(), checker, referenced, records );
    }

    public void checkDiffReference( REFERENCED oldReferenced, REFERENCED newReferenced, RecordAccess records )
    {
        ConsistencyReporter.dispatchChangeReference( report(), checker, oldReferenced, newReferenced, records );
    }

    public synchronized void skip()
    {
        if ( report != null )
        {
            ConsistencyReporter.dispatchSkip( report );
            report = null;
        }
    }

    private synchronized ConsistencyReport report()
    {
        if ( report == null )
        {
            throw new IllegalStateException( "Reference has already been checked." );
        }
        try
        {
            return report;
        }
        finally
        {
            report = null;
        }
    }
}
