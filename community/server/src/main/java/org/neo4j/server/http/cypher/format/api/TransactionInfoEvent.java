/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.http.cypher.format.api;

import java.net.URI;

/**
 * An event that provides information about the transaction used for processing the request.
 * <p>
 * This is always the last event in the event stream.
 */
public class TransactionInfoEvent implements OutputEvent
{

    private final TransactionNotificationState notification;
    private final URI commitUri;
    private final long expirationTimestamp;

    public TransactionInfoEvent( TransactionNotificationState notification, URI commitUri, long expirationTimestamp )
    {
        this.notification = notification;
        this.commitUri = commitUri;
        this.expirationTimestamp = expirationTimestamp;
    }

    @Override
    public Type getType()
    {
        return Type.TRANSACTION_INFO;
    }

    public TransactionNotificationState getNotification()
    {
        return notification;
    }

    /**
     * Gets a URI that can be used for committing the transaction.
     * <p>
     * {@code null} is returned if the transaction is not in 'commitable' state. In other words, the commit URI is returned only
     * when the transaction is in {@link TransactionNotificationState#OPEN} state.
     *
     * @return a URI for committing the transaction or {@code null}.
     */
    public URI getCommitUri()
    {
        return commitUri;
    }

    /**
     * Gets a timestamp when the transaction will be terminated.
     * <p>
     * {@code -1} is returned if the transaction is not in open state ({@link TransactionNotificationState#OPEN}).
     *
     * @return an expiration timestamp or {@code -1}.
     */
    public long getExpirationTimestamp()
    {
        return expirationTimestamp;
    }
}
