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

import static org.neo4j.gqlstatus.GqlStatusInfoCodes.STATUS_25N11;

import java.util.Arrays;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.graphdb.TransientFailureException;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.kernel.api.exceptions.Status;

public class TransactionConflictException extends TransientFailureException {

    private static final String GENERIC_MESSAGE = "Transaction conflict validation failed.";
    private static final ErrorGqlStatusObject GQL_STATUS =
            ErrorGqlStatusObjectImplementation.from(STATUS_25N11).build();
    private DatabaseFile databaseFile;
    private long observedVersion;
    private long highestClosed;

    private TransactionConflictException(
            ErrorGqlStatusObject gqlStatusObject,
            DatabaseFile databaseFile,
            VersionContext versionContext,
            long pageId) {
        super(gqlStatusObject, createMessage(databaseFile.getName(), pageId, versionContext));
        this.databaseFile = databaseFile;
        this.observedVersion = versionContext.chainHeadVersion();
        this.highestClosed = versionContext.highestClosed();
    }

    private TransactionConflictException(
            ErrorGqlStatusObject gqlStatusObject,
            DatabaseFile databaseFile,
            VersionContext versionContext,
            String message) {
        super(gqlStatusObject, message);
        this.databaseFile = databaseFile;
        this.observedVersion = versionContext.chainHeadVersion();
        this.highestClosed = versionContext.highestClosed();
    }

    private TransactionConflictException(ErrorGqlStatusObject gqlStatusObject, String message, Exception cause) {
        super(gqlStatusObject, message, cause);
    }

    private TransactionConflictException(ErrorGqlStatusObject gqlStatusObject, DatabaseFile databaseFile, long pageId) {
        super(gqlStatusObject, createPageIdPagedMessage(databaseFile.getName(), pageId));
        this.databaseFile = databaseFile;
    }

    public static TransactionConflictException transactionConflict(
            DatabaseFile databaseFile, VersionContext versionContext, long pageId) {
        return new TransactionConflictException(GQL_STATUS, databaseFile, versionContext, pageId);
    }

    public static TransactionConflictException uniqueIndexEntryConflict(
            String indexName, VersionContext versionContext) {
        return new TransactionConflictException(GQL_STATUS, createMessageUniqueIndex(indexName, versionContext), null);
    }

    public static TransactionConflictException transactionConflict(Exception cause) {
        return new TransactionConflictException(GQL_STATUS, GENERIC_MESSAGE, cause);
    }

    public static TransactionConflictException transactionConflict(DatabaseFile databaseFile, long pageId) {
        return new TransactionConflictException(GQL_STATUS, databaseFile, pageId);
    }

    public static TransactionConflictException denseRelationshipTransactionConflict(
            DatabaseFile denseDatabaseFile, VersionContext versionContext, long denseRelationshipId) {
        return new TransactionConflictException(
                GQL_STATUS,
                denseDatabaseFile,
                versionContext,
                createMessageDense(denseDatabaseFile.getName(), denseRelationshipId, versionContext));
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

    private static String createMessageDense(
            String denseRelationshipStoreName, long denseRelationshipId, VersionContext versionContext) {
        return "Concurrent modification exception. Dense relationship " + denseRelationshipId + " in '"
                + denseRelationshipStoreName + "' store is modified already by transaction "
                + versionContext.chainHeadVersion() + ", while ongoing transaction highest visible is: "
                + versionContext.highestClosed()
                + ", with not yet visible transaction ids are: "
                + Arrays.toString(versionContext.notVisibleTransactionIds()) + ".";
    }

    private static String createMessageUniqueIndex(String indexName, VersionContext versionContext) {
        return "Concurrent modification exception. Matching entry in unique index " + indexName
                + " is already created by transaction "
                + versionContext.chainHeadVersion() + ", while ongoing transaction highest visible is: "
                + versionContext.highestClosed()
                + ", with not yet visible transaction ids are: "
                + Arrays.toString(versionContext.notVisibleTransactionIds()) + ".";
    }
}
