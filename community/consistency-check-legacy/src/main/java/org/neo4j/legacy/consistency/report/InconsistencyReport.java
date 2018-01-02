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
package org.neo4j.legacy.consistency.report;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.legacy.consistency.RecordType;

public class InconsistencyReport implements InconsistencyLogger
{
    private final InconsistencyLogger logger;
    private final ConsistencySummaryStatistics summary;

    public InconsistencyReport( InconsistencyLogger logger, ConsistencySummaryStatistics summary )
    {
        this.logger = logger;
        this.summary = summary;
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord record, String message, Object[] args )
    {
        logger.error( recordType, record, message, args );
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                       String message, Object[] args )
    {
        logger.error( recordType, oldRecord, newRecord, message, args );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord record, String message, Object[] args )
    {
        logger.warning( recordType, record, message, args );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                         String message, Object[] args )
    {
        logger.warning( recordType, oldRecord, newRecord, message, args );
    }

    void updateSummary( RecordType type, int errors, int warnings )
    {
        summary.update( type, errors, warnings );
    }
}
