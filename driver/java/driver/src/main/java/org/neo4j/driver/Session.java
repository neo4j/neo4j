/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver;

/**
 * A live session with a Neo4j instance.
 * <p>
 * Sessions serve two purposes. For one, they are an optimization. By keeping state on the database side, we can
 * avoid re-transmitting certain metadata over and over.
 * <p>
 * Above that, Sessions serve a role transaction isolation and ordering semantics. Neo4j requires
 * "sticky sessions", meaning all requests within one session must always go to the same Neo4j instance.
 * <p>
 * With that requirement, Neo4j can achieve ACID semantics in a scalable manner even in large database clusters.
 * <p>
 * Session objects are not thread safe, if you want to run concurrent operations against the database,
 * simply create multiple sessions objects.
 */
public interface Session extends AutoCloseable, StatementRunner
{
    /**
     * Begin a new transaction in this session. A session can have at most one transaction running at a time, if you
     * want to run multiple concurrent transactions, you should use multiple concurrent sessions.
     * <p>
     * All data operations in Neo4j are transactional. However, for convenience we provide a {@link #run(String)}
     * method directly on this session interface as well. When you use that method, your statement automatically gets
     * wrapped in a transaction.
     * <p>
     * If you want to run multiple statements in the same transaction, you should wrap them in a transaction using this
     * method.
     *
     * @return a new transaction
     */
    Transaction newTransaction();

    @Override
    void close();
}