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
package org.neo4j.internal.kernel.api;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;

/**
 * The Kernel.
 */
public interface Kernel
{
    /**
     * Begin new transaction.
     *
     * @param type type of transaction (implicit/explicit)
     * @param loginContext the {@link LoginContext} of the user which is beginning this transaction
     * @param clientInfo {@link ClientConnectionInfo} of the user which is beginning this transaction
     * @return the transaction
     */
    <T extends Transaction> T beginTransaction( Transaction.Type type, LoginContext loginContext, ClientConnectionInfo clientInfo )
            throws TransactionFailureException;

    /**
     * Begin new transaction.
     *
     * @param type type of transaction (implicit/explicit)
     * @param loginContext the {@link LoginContext} of the user which is beginning this transaction
     * @return the transaction
     */
    <T extends Transaction> T beginTransaction( Transaction.Type type, LoginContext loginContext )
            throws TransactionFailureException;

    /**
     * Cursor factory which produces cursors that are not bound to any particular transaction.
     */
    CursorFactory cursors();
}
