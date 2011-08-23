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
package org.neo4j.graphdb;

/**
 * A programmatically handled transaction.
 * <em>All modifying operations that work with the
 * node space must be wrapped in a transaction.</em> Transactions are thread
 * confined. Transactions can either be handled programmatically, through this
 * interface, or by a container through the Java Transaction API (JTA). The
 * Transaction interface makes handling programmatic transactions easier than
 * using JTA programmatically. Here's the idiomatic use of programmatic
 * transactions in Neo4j:
 * 
 * <pre>
 * <code>
 * Transaction tx = graphDb.beginTx();
 * try
 * {
 * 	... // any operation that works with the node space
 *     tx.success();
 * }
 * finally
 * {
 *     tx.finish();
 * }
 * </code>
 * </pre>
 * <p>
 * Let's walk through this example line by line. First we retrieve a Transaction
 * object by invoking the {@link GraphDatabaseService#beginTx()} factory method.
 * This creates a new Transaction instance which has internal state to keep
 * track of whether the current transaction is successful. Then we wrap all
 * operations that work with the node space in a try-finally block. At the end
 * of the block, we invoke the {@link #finish() tx.success()} method to indicate
 * that the transaction is successful. As we exit the block, the finally clause
 * will kick in and {@link #finish() tx.finish} will commit the transaction if
 * the internal state indicates success or else mark it for rollback.
 * <p>
 * If an exception is raised in the try-block, {@link #success()} will never be
 * invoked and the internal state of the transaction object will cause
 * {@link #finish()} to roll back the transaction. This is very important:
 * unless {@link #success()} is invoked, the transaction will fail upon
 * {@link #finish()}. A transaction can be explicitly marked for rollback by
 * invoking the {@link #failure()} method.
 * <p>
 * Read operations inside of a transaction will also read uncommitted data from
 * the same transaction.
 */
public interface Transaction
{
    /**
     * Marks this transaction as failed, which means that it will
     * unconditionally be rolled back when {@link #finish()} is called. Once
     * this method has been invoked, it doesn't matter if
     * {@link #success()} is invoked afterwards -- the transaction will still be 
     * rolled back.
     */
    public void failure();

    /**
     * Marks this transaction as successful, which means that it will be
     * committed upon invocation of {@link #finish()} unless {@link #failure()}
     * has or will be invoked before then.
     */
    public void success();

    /**
     * Commits or marks this transaction for rollback, depending on whether
     * {@link #success()} or {@link #failure()} has been previously invoked.
     */
    public void finish();
}
