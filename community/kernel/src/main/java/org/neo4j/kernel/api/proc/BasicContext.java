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

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.ValueMapper;

import static java.util.Objects.requireNonNull;

public class BasicContext implements Context
{
    private final DependencyResolver resolver;
    private final KernelTransaction kernelTransaction;
    private final SecurityContext securityContext;
    private final ValueMapper<Object> valueMapper;
    private final Thread thread;

    private BasicContext( DependencyResolver resolver,
            KernelTransaction kernelTransaction,
            SecurityContext securityContext, ValueMapper<Object> valueMapper,
            Thread thread )
    {
        this.resolver = resolver;
        this.kernelTransaction = kernelTransaction;
        this.securityContext = securityContext;
        this.valueMapper = valueMapper;
        this.thread = thread;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <T> T get( Key<T> key ) throws ProcedureException
    {
        switch ( key.name() )
        {
        case VALUE_MAPPER_NAME:
            return (T) valueMapper;
        case DEPENDENCY_RESOLVER_NAME:
            return (T) resolver;
        case DATABASE_API_NAME:
            return (T) resolver.resolveDependency( GraphDatabaseAPI.class );
        case KERNEL_TRANSACTION_NAME:
            return throwIfNull( key, kernelTransaction );
        case SECURITY_CONTEXT_NAME:
            return (T) securityContext;
        case THREAD_NAME:
            return (T) thread;
        case SYSTEM_CLOCK_NAME:
            return throwIfNull( key, kernelTransaction, t -> (T)t.clocks().systemClock() );
        case STATEMENT_CLOCK_NAME:
            return throwIfNull( key, kernelTransaction,  t -> (T)t.clocks().statementClock());
        case TRANSACTION_CLOCK_NAME:
            return throwIfNull( key, kernelTransaction,  t -> (T)t.clocks().transactionClock() );
        default:
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "There is no `%s` in the current procedure call context.", key.name() );
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
            return getOrElse( resolver, orElse, r -> (T) r.resolveTypeDependencies( GraphDatabaseAPI.class ) );
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

    @Override
    public ValueMapper<Object> valueMapper()
    {
        return valueMapper;
    }

    public static ContextBuilder buildContext( DependencyResolver dependencyResolver, ValueMapper<Object> valueMapper )
    {
        return new ContextBuilder( dependencyResolver, valueMapper );
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
        private Thread thread = Thread.currentThread();
        private ValueMapper<Object> valueMapper;

        private ContextBuilder( DependencyResolver resolver, ValueMapper<Object> valueMapper )
        {
            this.resolver = resolver;
            this.valueMapper = valueMapper;
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
            requireNonNull( resolver );
            requireNonNull( securityContext );
            requireNonNull( valueMapper );
            requireNonNull( thread );
            return new BasicContext( resolver, kernelTransaction, securityContext, valueMapper, thread );
        }

    }
}
