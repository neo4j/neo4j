/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.bind;

import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import javax.ws.rs.core.Context;

/**
 * Jersey dependency injection configuration.
 * Used by {@link ResourceConfig} to know how to deal with fields annotated with {@link Context}.
 */
public class ComponentsBinder extends AbstractBinder
{
    private final List<SingletonBinding> singletonBindings = new ArrayList<>();
    private final List<LazyBinding> lazyBindings = new ArrayList<>();
    private final List<LazyFactoryBinding<?>> lazyFactoryBindings = new ArrayList<>();

    private boolean configured;

    /**
     * Register a component for dependency injection. The same object will always be injected where the given type is required.
     *
     * @param component the singleton object to be injected.
     * @param type the type of the field where the singleton needs to be injected.
     * @param <T> type of the component.
     */
    public <T> void addSingletonBinding( T component, Class<T> type )
    {
        verifyBinding( component, type );
        singletonBindings.add( new SingletonBinding( component, type ) );
    }

    /**
     * Register a supplier that provides a component for dependency injection. Supplier will be invoked for every injection attempt.
     *
     * @param componentSupplier the component supplier.
     * @param type the type of the field where the provided component needs to be injected.
     */
    public void addLazyBinding( Supplier<?> componentSupplier, Class<?> type )
    {
        verifyBinding( componentSupplier, type );
        lazyBindings.add( new LazyBinding( componentSupplier, type ) );
    }

    /**
     * Register a supplier type that provides a component for dependency injection. Supplier will be created once and invoked for every injection attempt.
     * Container is responsible for instantiation of the supplier and can inject dependencies into it.
     *
     * @param supplierType the type of the components supplier.
     * @param type the type of the field where the provided component needs to be injected.
     * @param <T> type of the component.
     */
    public <T> void addLazyBinding( Class<? extends Supplier<T>> supplierType, Class<T> type )
    {
        verifyBinding( supplierType, type );
        lazyFactoryBindings.add( new LazyFactoryBinding<>( supplierType, type ) );
    }

    @Override
    protected void configure()
    {
        if ( configured )
        {
            return;
        }
        configured = true;

        for ( SingletonBinding binding : singletonBindings )
        {
            bind( binding.component() ).to( binding.type() );
        }

        for ( LazyBinding binding : lazyBindings )
        {
            bindFactory( binding.supplier() ).to( binding.type() );
        }

        for ( LazyFactoryBinding<?> binding : lazyFactoryBindings )
        {
            bindFactory( binding.supplierType() ).to( binding.type() );
        }
    }

    private void verifyBinding( Object binding, Class<?> type )
    {
        Objects.requireNonNull( binding, "binding" );
        Objects.requireNonNull( type, "type" );

        if ( configured )
        {
            throw new IllegalStateException( "Unable to add new binding. Binder has already been configured" );
        }
    }
}
