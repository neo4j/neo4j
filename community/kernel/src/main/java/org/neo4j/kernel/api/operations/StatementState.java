/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.operations;

import java.io.Closeable;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.LockHolder;
import org.neo4j.kernel.impl.api.state.TxState;

/**
 * Contains all state necessary for satisfying operations performed on a statement.
 * 
 * There's a possibility that this object, since it's built by {@link KernelTransaction#newStatementState()},
 * can be generic and be decorated with whatever state objects the layers in the {@link KernelTransaction}
 * needs. But for now I'd say it's enough with a specific cake knowing the layout of the cake.
 * Also if going specific cake then the top most layer can be hard coded to return a new such instance directly.
 * 
 * @author Mattias Persson
 */
public interface StatementState extends TxState.Holder, Closeable
{
    LockHolder locks();

    IndexReaderFactory indexReaderFactory();

    @Override
    void close();
}
