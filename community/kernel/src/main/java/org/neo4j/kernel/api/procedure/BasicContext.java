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
package org.neo4j.kernel.api.procedure;

import java.time.Clock;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
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
    private final ProcedureCallContext procedureCallContext;

    private BasicContext( DependencyResolver resolver,
            KernelTransaction kernelTransaction,
            SecurityContext securityContext, ValueMapper<Object> valueMapper,
            Thread thread, ProcedureCallContext procedureCallContext )
    {
        this.resolver = resolver;
        this.kernelTransaction = kernelTransaction;
        this.securityContext = securityContext;
        this.valueMapper = valueMapper;
        this.thread = thread;
        this.procedureCallContext = procedureCallContext;
    }

    @Override
    public ValueMapper<Object> valueMapper()
    {
        return valueMapper;
    }

    @Override
    public SecurityContext securityContext()
    {
        return securityContext;
    }

    @Override
    public DependencyResolver dependencyResolver()
    {
        return resolver;
    }

    @Override
    public GraphDatabaseAPI graphDatabaseAPI()
    {
        return resolver.resolveDependency( GraphDatabaseAPI.class );
    }

    @Override
    public Thread thread()
    {
        return thread;
    }

    @Override
    public KernelTransaction kernelTransaction() throws ProcedureException
    {
        return throwIfNull( "KernelTransaction", kernelTransaction );
    }

    @Override
    public KernelTransaction kernelTransactionOrNull()
    {
        return kernelTransaction;
    }

    @Override
    public Clock systemClock() throws ProcedureException
    {
        return throwIfNull( "SystemClock", kernelTransaction, t -> t.clocks().systemClock() );
    }

    @Override
    public Clock statementClock() throws ProcedureException
    {
        return throwIfNull( "StatementClock", kernelTransaction, t -> t.clocks().statementClock() );
    }

    @Override
    public Clock transactionClock() throws ProcedureException
    {
        return throwIfNull( "TransactionClock", kernelTransaction, t -> t.clocks().transactionClock() );
    }

    @Override
    public ProcedureCallContext procedureCallContext()
    {
        return procedureCallContext;
    }

    public static ContextBuilder buildContext( DependencyResolver dependencyResolver, ValueMapper<Object> valueMapper )
    {
        return new ContextBuilder( dependencyResolver, valueMapper );
    }

    @SuppressWarnings( "unchecked" )
    private <T, U> T throwIfNull( String name, U value ) throws ProcedureException
    {
        return throwIfNull( name, value, v -> (T) v );
    }

    private <T, U> T throwIfNull( String name, U value, Function<U,T> producer ) throws ProcedureException
    {
        if ( value == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "There is no `%s` in the current procedure call context.", name );
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
        private ProcedureCallContext procedureCallContext;

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

        public ContextBuilder withProcedureCallContext( ProcedureCallContext procedureContext )
        {
            this.procedureCallContext = procedureContext;
            return this;
        }

        public Context context()
        {
            requireNonNull( resolver );
            requireNonNull( securityContext );
            requireNonNull( valueMapper );
            requireNonNull( thread );
            return new BasicContext( resolver, kernelTransaction, securityContext, valueMapper, thread, procedureCallContext );
        }
    }
}
