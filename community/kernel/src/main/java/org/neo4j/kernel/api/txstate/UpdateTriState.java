/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.txstate;

/**
 * Represents the three states that updates can assume in a transaction:
 * <ul>
 * <li>Either the data can be {@linkplain #ADDED added} in this transaction,</li>
 * <li>the data can be {@linkplain #REMOVED removed} by this transaction, or</li>
 * <li>the data can be {@linkplain #UNTOUCHED untouched} by this transaction,
 *     having whatever state it has in the underlying store.</li>
 * </ul>
 */
public enum UpdateTriState
{
    ADDED,
    REMOVED,
    UNTOUCHED;
}
