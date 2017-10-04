/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;

class NeoStoreTransactionLogModule
{
    private final LogicalTransactionStore logicalTransactionStore;
    private final LogFileInformation logFileInformation;
    private final PhysicalLogFiles logFiles;
    private final LogFile logFile;
    private final LogRotation logRotation;
    private final CheckPointerImpl checkPointer;
    private final TransactionAppender appender;
    private final SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering;

    NeoStoreTransactionLogModule( LogicalTransactionStore logicalTransactionStore,
            LogFileInformation logFileInformation, PhysicalLogFiles logFiles, LogFile logFile, LogRotation logRotation,
            CheckPointerImpl checkPointer, TransactionAppender appender,
            SynchronizedArrayIdOrderingQueue explicitIndexTransactionOrdering )
    {
        this.logicalTransactionStore = logicalTransactionStore;
        this.logFileInformation = logFileInformation;
        this.logFiles = logFiles;
        this.logFile = logFile;
        this.logRotation = logRotation;
        this.checkPointer = checkPointer;
        this.appender = appender;
        this.explicitIndexTransactionOrdering = explicitIndexTransactionOrdering;
    }

    public LogicalTransactionStore logicalTransactionStore()
    {
        return logicalTransactionStore;
    }

    CheckPointer checkPointing()
    {
        return checkPointer;
    }

    TransactionAppender transactionAppender()
    {
        return appender;
    }

    public void satisfyDependencies( Dependencies dependencies )
    {
        dependencies.satisfyDependencies( checkPointer,
                                          logFile,
                                          logFileInformation,
                                          logFiles, explicitIndexTransactionOrdering,
                                          logicalTransactionStore,
                                          logRotation,
                                          appender );
    }
}
