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
package org.neo4j.kernel.api.proc;

import java.util.function.Function;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class BasicContext implements Context
{
    private final DependencyResolver resolver;
    private final KernelTransaction kernelTransaction;
    private final SecurityContext securityContext;
    private final Thread thread;

    private BasicContext( DependencyResolver resolver,
            KernelTransaction kernelTransaction,
            SecurityContext securityContext, Thread thread )
    {
        this.resolver = resolver;
        this.kernelTransaction = kernelTransaction;
        this.securityContext = securityContext;
        this.thread = thread;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T get( Key<T> key ) throws ProcedureException
    {
        switch ( key.name() )
        {
        case DEPENDENCY_RESOLVER_NAME:
            return throwIfNull( key, resolver );
        case DATABASE_API_NAME:
            return throwIfNull( key, resolver.resolveTypeDependencies( GraphDatabaseAPI.class ) );
        case KERNEL_TRANSACTION_NAME:
            return throwIfNull( key, kernelTransaction );
        case SECURITY_CONTEXT_NAME:
            return throwIfNull( key, securityContext );
        case THREAD_NAME:
            return throwIfNull( key, thread );
        case SYSTEM_CLOCK_NAME:
            return throwIfNull( key, kernelTransaction, t -> (T)t.clocks().systemClock() );
        case STATEMENT_CLOCK_NAME:
            return throwIfNull( key, kernelTransaction,  t -> (T)t.clocks().statementClock());
        case TRANSACTION_CLOCK_NAME:
            return throwIfNull( key, kernelTransaction,  t -> (T)t.clocks().transactionClock() );
        default:
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "There is no `%s` in the current procedure call context.", key.name() );
        }
    }
    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T getOrElse( Key<T> key, T orElse )
    {
        switch ( key.name() )
        {
        case DEPENDENCY_RESOLVER_NAME:
            return getOrElse( resolver, orElse );
        case DATABASE_API_NAME:
            return getOrElse( resolver.resolveTypeDependencies( GraphDatabaseAPI.class ), orElse );
        case KERNEL_TRANSACTION_NAME:
            return getOrElse( kernelTransaction, orElse );
        case SECURITY_CONTEXT_NAME:
            return getOrElse( securityContext, orElse );
        case THREAD_NAME:
            return getOrElse( thread, orElse );
        case SYSTEM_CLOCK_NAME:
            return getOrElse( kernelTransaction, orElse, t -> (T)t.clocks().systemClock() );
        case STATEMENT_CLOCK_NAME:
            return getOrElse( kernelTransaction, orElse, t -> (T)t.clocks().statementClock() );
        case TRANSACTION_CLOCK_NAME:
            return getOrElse( kernelTransaction, orElse, t -> (T)t.clocks().transactionClock() );
        default:
            return orElse;
        }
    }

    public static ContextBuilder buildContext()
    {
        return new ContextBuilder();
    }

    @SuppressWarnings( "unchecked" )
    private <T, U> T throwIfNull( Key<T> key, U value ) throws ProcedureException
    {
        return throwIfNull( key, value, v -> (T) v );
    }

    private <T, U> T throwIfNull( Key<T> key, U value, Function<U,T> producer ) throws ProcedureException
    {
        if ( value == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "There is no `%s` in the current procedure call context.", key.name() );
        }
        return producer.apply( value );
    }

    @SuppressWarnings( "unchecked" )
    private <T,U> T getOrElse( U value, T orElse )
    {
        return getOrElse( value, orElse, v -> (T)v );
    }

    private <T,U> T getOrElse( U value, T orElse, Function<U,T> producer )
    {
        if ( value == null )
        {
            return orElse;
        }
        return producer.apply( value );
    }

    public static class ContextBuilder
    {
        private DependencyResolver resolver;
        private KernelTransaction kernelTransaction;
        private SecurityContext securityContext = SecurityContext.AUTH_DISABLED;
        private Thread thread;

        public ContextBuilder withResolver( DependencyResolver resolver )
        {
            this.resolver = resolver;
            return this;
        }

        public ContextBuilder withKernelTransaction( KernelTransaction kernelTransaction )
        {
            this.kernelTransaction = kernelTransaction;
            return this;
        }

        public ContextBuilder withSecurityContext( SecurityContext securityContext )
        {
            this.securityContext = securityContext;
            return this;
        }

        public ContextBuilder withThread( Thread thread )
        {
            this.thread = thread;
            return this;
        }

        public Context context()
        {
            return new BasicContext( resolver, kernelTransaction, securityContext, thread );
        }

    }
}
