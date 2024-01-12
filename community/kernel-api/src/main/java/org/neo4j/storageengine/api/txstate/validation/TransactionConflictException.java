/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.txstate.validation;

import java.util.Arrays;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.api.exceptions.Status;

public class TransactionConflictException extends TransientFailureException {

    private static final String GENERIC_MESSAGE = "Transaction conflict validation failed.";
    private DatabaseFile databaseFile;
    private long observedVersion;
    private long highestClosed;

    public TransactionConflictException(DatabaseFile databaseFile, VersionContext versionContext, long pageId) {
        super(createMessage(databaseFile.getName(), pageId, versionContext));
        this.databaseFile = databaseFile;
        this.observedVersion = versionContext.chainHeadVersion();
        this.highestClosed = versionContext.highestClosed();
    }

    public TransactionConflictException(String message, Exception cause) {
        super(message, cause);
    }

    public TransactionConflictException(Exception cause) {
        this(GENERIC_MESSAGE, cause);
    }

    public TransactionConflictException(RecordDatabaseFile databaseFile, long pageId) {
        super(createPageIdPagedMessage(databaseFile.getName(), pageId));
        this.databaseFile = databaseFile;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public DatabaseFile getDatabaseFile() {
        return databaseFile;
    }

    public long getObservedVersion() {
        return observedVersion;
    }

    public long getHighestClosed() {
        return highestClosed;
    }

    @Override
    public Status status() {
        return Status.Transaction.Outdated;
    }

    private static String createPageIdPagedMessage(String databaseFileName, long pageId) {
        return "Concurrent modification exception. Page " + pageId + " in '" + databaseFileName
                + "' store is already locked by other transaction validator.";
    }

    private static String createMessage(String databaseFileName, long pageId, VersionContext versionContext) {
        return "Concurrent modification exception. Page " + pageId + " in '"
                + databaseFileName + "' store is modified already by transaction "
                + versionContext.chainHeadVersion() + ", while ongoing transaction highest visible is: "
                + versionContext.highestClosed()
                + ", with not yet visible transaction ids are: "
                + Arrays.toString(versionContext.notVisibleTransactionIds()) + ".";
    }
}
