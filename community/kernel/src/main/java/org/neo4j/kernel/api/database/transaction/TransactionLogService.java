/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.database.transaction;

import java.io.IOException;

/**
 * Service to access database transaction logs
 */
public interface TransactionLogService
{
    /**
     * Provide list of channels for underlying transaction log files starting with requested initial transaction id.
     * Provided channels can be closed for any reason by underlying database, if it will choose to do so in any moment.
     * Its client responsibility to repeat request in such cases to retrieve new set of readers.
     *
     * @param initialTxId initial transaction id to retrieve channels from. Should be positive number and be in range of existing transaction ids.
     * @return object with access to read only transaction log channels to be able to access requested transaction logs content
     * @throws IOException on failure performing underlying transaction logs operation
     * @throws IllegalArgumentException invalid transaction id
     */
    TransactionLogChannels logFilesChannels( long initialTxId ) throws IOException;
}
