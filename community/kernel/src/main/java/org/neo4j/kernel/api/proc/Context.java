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
package org.neo4j.kernel.api.proc;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * The context in which a procedure is invoked. This is a read-only map-like structure.
 * For instance, a read-only transactional procedure might have access to the current statement it is being invoked
 * in through this.
 *
 * The context is entirely defined by the caller of the procedure,
 * so what is available in the context depends on the context of the call.
 */
public interface Context
{
    Key<KernelTransaction> KERNEL_TRANSACTION = Key.key( "KernelTransaction", KernelTransaction.class );
    Key<SecurityContext> SECURITY_CONTEXT = Key.key( "SecurityContext", SecurityContext.class );
    Key<Thread> THREAD = Key.key( "Thread", Thread.class );

    <T> T get( Key<T> key ) throws ProcedureException;
    <T> T getOrElse( Key<T> key, T orElse );
}
