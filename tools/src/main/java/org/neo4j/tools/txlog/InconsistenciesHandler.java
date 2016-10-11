/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.tools.txlog;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Handler of inconsistencies discovered by {@link CheckTxLogs} tool.
 */
interface InconsistenciesHandler
{
    /**
     * For reporting of invalid check points.
     * @param logVersion the log file version where the check point is located in
     * @param logPosition the invalid logPosition stored in the check point entry
     * @param size the size of file pointed by the check point entry
     */
    void reportInconsistentCheckPoint( long logVersion, LogPosition logPosition, long size );

    /**
     * For reporting of inconsistencies found between before and after state of commands.
     * @param committed the record seen previously during transaction log scan and considered valid
     * @param current the record met during transaction log scan and considered inconsistent with committed
     */
    void reportInconsistentCommand( RecordInfo<?> committed, RecordInfo<?> current );

    /**
     * For reporting of inconsistencies found about tx id sequences
     * @param lastSeenTxId last seen tx id before processing the current commit
     * @param currentTxId the transaction id of the process commit entry
     */
    void reportInconsistentTxIdSequence( long lastSeenTxId, long currentTxId );
}
