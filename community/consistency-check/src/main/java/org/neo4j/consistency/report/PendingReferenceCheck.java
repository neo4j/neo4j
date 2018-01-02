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
package org.neo4j.consistency.report;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class PendingReferenceCheck<REFERENCED extends AbstractBaseRecord>
{
    private CheckerEngine engine;
    private final ComparativeRecordChecker checker;

    PendingReferenceCheck( CheckerEngine engine, ComparativeRecordChecker checker )
    {
        this.engine = engine;
        this.checker = checker;
    }

    @Override
    public synchronized String toString()
    {
        if ( engine == null )
        {
            return String.format( "CompletedReferenceCheck{%s}", checker );
        }
        else
        {
            return ConsistencyReporter.pendingCheckToString( engine, checker );
        }
    }

    public void checkReference( REFERENCED referenced, RecordAccess records )
    {
        ConsistencyReporter.dispatchReference( engine(), checker, referenced, records );
    }

    public void checkDiffReference( REFERENCED oldReferenced, REFERENCED newReferenced, RecordAccess records )
    {
        ConsistencyReporter.dispatchChangeReference( engine(), checker, oldReferenced, newReferenced, records );
    }

    public synchronized void skip()
    {
        if ( engine != null )
        {
            ConsistencyReporter.dispatchSkip( engine );
            engine = null;
        }
    }

    private synchronized CheckerEngine engine()
    {
        if ( engine == null )
        {
            throw new IllegalStateException( "Reference has already been checked." );
        }
        try
        {
            return engine;
        }
        finally
        {
            engine = null;
        }
    }
}
