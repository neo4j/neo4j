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
package org.neo4j.bolt.transaction;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.messaging.ResultConsumer;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.values.virtual.MapValue;

/**
 * Interface defines transaction management capabilities and the boundary between the bolt/http servers
 * and the lower level components of the server.
 */
public interface TransactionManager
{
    /**
     * Begin a new transaction.
     *
     * @param defaultDb           the default database to execute the transaction against.
     * @param bookmarks           the bookmark requested to use for this transaction.
     * @param isReadOnly          whether the transaction is read-only or not.
     * @param transactionMetadata metadata for this transaction.
     * @param transactionTimeout  how long to wait before this transaction will timeout.
     * @param connectionId        the connection that this transaction is tied to (for future removal).
     * @return the id that uniquely identifies the transaction.
     * @throws KernelException General error that can occur during transaction creation.
     * @deprecated currently requires {@code connectionId} which will be replaced with {@link #begin(String, List, boolean, Map, Duration)}.
     */
    @Deprecated
    String begin( String defaultDb, List<Bookmark> bookmarks, boolean isReadOnly, Map<String,Object> transactionMetadata, Duration transactionTimeout,
                  String connectionId )
            throws KernelException;

    /**
     * Begin a new transaction.
     *
     * @param defaultDb           the default database to execute the transaction against.
     * @param bookmarks           the bookmark requested to use for this transaction.
     * @param isReadOnly          whether the transaction is read-only or not.
     * @param transactionMetadata metadata for this transaction.
     * @param transactionTimeout  how long to wait before this transaction will timeout.
     * @return the id that uniquely identifies the transaction.
     * @throws KernelException General error that can occur during transaction creation.
     */
    default String begin( String defaultDb, List<Bookmark> bookmarks, boolean isReadOnly, Map<String,Object> transactionMetadata, Duration transactionTimeout )
            throws KernelException
    {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    /**
     * Commit a transaction.
     * @param txId the identifier of the transaction to be committed.
     * @return A {@link Bookmark} marking the position at which the transaction was committed.
     * @throws KernelException A general error which can occur on committing the transaction.
     * @throws TransactionNotFoundException thrown if a transaction cannot be found in this transaction manager with the identifier provided.
     */
    Bookmark commit( String txId ) throws KernelException, TransactionNotFoundException;

    /**
     * Run a "Cypher Program" outside of transactional context.
     * Executing Cypher this way allows the Cypher engine to manage the transaction itself.
     * @param defaultDb the default database to use for this program.
     * @param cypherProgram the cypher program to be executed.
     * @param params the parameters to use in the cypher program.
     * @param bookmarks the bookmark requested to use for this program.
     * @param isReadOnly whether the program is read-only or not.
     * @param programMetadata metadata for this program.
     * @param programTimeout how long to wait before this program will timeout.
     * @param connectionId the connection that this transaction is tied to (for future removal).
     * @return A {@link DefaultProgramResultReference} which contains the transaction identifier associated with this program and a
     * {@link StatementMetadata} with details of program that are known at execution time.
     * @throws KernelException A general error which can occur on running a query within a transaction.
     */
    ProgramResultReference runProgram( String defaultDb, String cypherProgram, MapValue params, List<Bookmark> bookmarks,
                                       boolean isReadOnly, Map<String,Object> programMetadata, Duration programTimeout,
                                       String connectionId ) throws KernelException;

    /**
     * Rollback a transaction.
     * @param txId the identifier of the transaction to be rolled back.
     * @throws TransactionNotFoundException thrown if a transaction cannot be found in this transaction manager with the identifier provided.
     */
    void rollback( String txId ) throws TransactionNotFoundException;

    /**
     * Run a query within a transactional context.
     * @param txId the transaction to execute the query in.
     * @param cypherQuery the cypher query string to be executed.
     * @param params the parameters to use in the cypherQuery
     * @return {@link StatementMetadata} containing known details of the query at execution time.
     * @throws KernelException A general error which can occur on running a query within a transaction.
     * @throws TransactionNotFoundException thrown if a transaction cannot be found in this transaction manager with the identifier provided.
     */
    StatementMetadata runQuery( String txId, String cypherQuery, MapValue params ) throws KernelException, TransactionNotFoundException;//todo statementMetadata

    /**
     * Process up to {@code numberToPull} items from a previously executed query and return them asynchronously to a consumer.
     * @param txId the transaction identifier to pull the data from.
     * @param statementId the statement identifier within the transaction to pull the data from.
     * @param numberToPull the number of data items to pull.
     * @param consumer the consumer that will asynchronously receive the data.
     * @return A {@link Bookmark} marking the position at which the transaction was pulled.
     * @throws ResultNotFoundException thrown if the provided statementId for this transaction was not found.
     * @throws AuthorizationExpiredException thrown when required authorization info has expired in the Neo4j auth cache.
     * @throws TransactionNotFoundException thrown if a transaction cannot be found in this transaction manager with the identifier provided.
     */
    Bookmark pullData( String txId, int statementId, long numberToPull, ResultConsumer consumer ) throws ResultNotFoundException, //todo Replace ResultConsumer
                                                                                                     AuthorizationExpiredException,
                                                                                                     TransactionNotFoundException;

    /**
     * Process up to {@code numberToDiscard} items from a previously executed query and discard them. //todo replace consumer .Used here for `hasMore` callback.
     * @param txId the transaction identifier to discard the data from.
     * @param statementId the statement identifier within the transaction to discard the data from.
     * @param numberToDiscard the number of data items to discard.
     * @param consumer the consumer that will asynchronously discard the data.
     * @return A {@link Bookmark} marking the position at which the transaction was pulled.
     * @throws ResultNotFoundException thrown if the provided statementId for this transaction was not found.
     * @throws AuthorizationExpiredException thrown when required authorization info has expired in the Neo4j auth cache.
     * @throws TransactionNotFoundException thrown if a transaction cannot be found in this transaction manager with the identifier provided.
     */
    Bookmark discardData( String txId, int statementId, long numberToDiscard, ResultConsumer consumer ) throws ResultNotFoundException,
                                                                                                           AuthorizationExpiredException,
                                                                                                           TransactionNotFoundException;

    /**
     * Cancel all remaining data for a previously executed query.
     * @param txId the transaction identifier to cancel the data from.
     * @param statementId the statement identifier within the transaction to cancel the data from.
     * @throws ResultNotFoundException thrown if the provided statementId for this transaction was not found.
     * @throws TransactionNotFoundException thrown if a transaction cannot be found in this transaction manager with the identifier provided.
     */
    void cancelData( String txId, int statementId ) throws ResultNotFoundException, TransactionNotFoundException;

    /**
     * Mark a transaction for termination.
     * @param txId the transaction identifier to interrupt.
     */
    void interrupt( String txId );

    /**
     * Return the status of a transaction
     * @return the current state of the transaction. See {@link TransactionStatus} for possible values.
     */
    TransactionStatus transactionStatus( String txId );

    /**
     * Initialized a resource to use in this Transaction Manager.
     * @param initializeContext context containing resources to be added.
     */
    void initialize( InitializeContext initializeContext );

    /**
     * Clean up resources for this Transaction Manager.
     * @param cleanUpContext context contain resources needed to be cleaned up.
     */
    void cleanUp( CleanUpContext cleanUpContext );

}
