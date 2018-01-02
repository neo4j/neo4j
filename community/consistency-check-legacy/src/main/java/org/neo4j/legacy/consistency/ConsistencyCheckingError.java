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
package org.neo4j.legacy.consistency;

import org.neo4j.kernel.impl.store.DataInconsistencyError;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.legacy.consistency.report.ConsistencySummaryStatistics;

public class ConsistencyCheckingError extends DataInconsistencyError
{
    private final ConsistencySummaryStatistics summary;

    public ConsistencyCheckingError( LogEntryStart startEntry, LogEntryCommit commitEntry,
                                     ConsistencySummaryStatistics summary )
    {
        super( String.format( "Inconsistencies in transaction:\n\t%s\n\t%s\n\t%s",
                              (startEntry == null ? "NO START ENTRY" : startEntry.toString()),
                              (commitEntry == null ? "NO COMMIT ENTRY" : commitEntry.toString()),
                              summary ) );
        this.summary = summary;
    }

    public int getInconsistencyCountForRecordType( RecordType recordType )
    {
        return summary.getInconsistencyCountForRecordType( recordType );
    }

    public int getTotalInconsistencyCount()
    {
        return summary.getTotalInconsistencyCount();
    }
}
