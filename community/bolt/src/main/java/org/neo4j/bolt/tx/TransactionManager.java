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
package org.neo4j.bolt.tx;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.kernel.impl.query.NotificationConfiguration;

public interface TransactionManager {

    /**
     * Retrieves the total number of transactions currently present within this transaction manager.
     *
     * @return a transaction
     */
    int getTransactionCount();

    /**
     * Retrieves a transaction based on its globally unique identifier.
     *
     * @param id a transaction identifier.
     * @return a transaction or null, if none with that identifier exists.
     */
    Optional<Transaction> get(String id);

    /**
     * Creates an explicit transaction capable of encapsulating the state of one or more statements.
     *
     * @param type                type of transaction.
     * @param owner               owner of transaction.
     * @param databaseName        name of which database the transaction will be executed.
     * @param mode                an access mode hint.
     * @param bookmarks           which bookmarks are required to execute this transaction.
     * @param timeout             a maximum transaction lifetime.
     * @param metadata            a map of user defined metadata.
     * @param notificationsConfig which notifications the transaction can create.
     * @return a transaction.
     */
    Transaction create(
            TransactionType type,
            TransactionOwner owner,
            String databaseName,
            AccessMode mode,
            List<String> bookmarks,
            Duration timeout,
            Map<String, Object> metadata,
            NotificationConfiguration notificationsConfig)
            throws TransactionException;
}
