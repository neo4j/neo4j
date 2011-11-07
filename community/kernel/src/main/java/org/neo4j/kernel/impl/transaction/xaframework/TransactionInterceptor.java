/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import org.neo4j.kernel.impl.nioneo.xa.CommandRecordVisitor;

/**
 * A TransactionInterceptor has the opportunity to perform a check on a
 * transaction before it touches the store and logical log, potentially
 * interrupting the process by throwing an exception. The initial idea
 * around this functionality was a consistency checking implementation but
 * any sort of run over the commands that comprise the transaction can be
 * done. Extending {@link CommandRecordVisitor} enables for visiting all
 * the records in the transaction and perform whatever work is necessary.
 *
 * TransactionInterceptors are instantiated by
 * {@link TransactionInterceptorProvider}s and are possible to form a chain
 * of responsibility.
 */
public interface TransactionInterceptor extends CommandRecordVisitor
{
    /**
     * The main work method, supposed to be called by users when the whole
     * required set of Commands has been met.
     * The last operation in a normal completion scenario for this method
     * must be calling complete() on the following member of the chain, if
     * present.
     */
    public void complete();

    /**
     * Set, if available, the log start entry for this transaction. The
     * implementation is not expected to act on it in any meaningful way
     * but it is required to pass it on in the chain before throwing it
     * out. Also, the implementation should not count on it being set
     * during its lifetime - it is possible that it is not available.
     * 
     * @param entry The start log entry for this transaction
     */
    public void setStartEntry( LogEntry.Start entry );

    /**
     * Set, if available, the log commit entry for this transaction. The
     * implementation is not expected to act on it in any meaningful way
     * but it is required to pass it on in the chain before throwing it
     * out. Also, the implementation should not count on it being set
     * during its lifetime - it is possible that it is not available.
     * 
     * @param entry The commit log entry for this transaction
     */
    public void setCommitEntry( LogEntry.Commit entry );
}
