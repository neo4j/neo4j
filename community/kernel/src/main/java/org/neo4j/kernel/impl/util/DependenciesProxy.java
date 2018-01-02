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
package org.neo4j.kernel.impl.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;

import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;

/**
 * Used to create dynamic proxies that implement dependency interfaces. Each method should have no arguments
 * and return the type of the dependency desired. It will be mapped to a lookup in the provided {@link DependencyResolver}.
 * Methods may also use a {@link Supplier} type for deferred lookups.
 *
 */
public class DependenciesProxy
{
    /**
     * Create a dynamic proxy that implements the given interface and backs invocation with lookups into the given
     * dependency resolver.
     *
     * @param dependencyResolver
     * @param dependenciesInterface
     * @param <T>
     * @return
     */
    public static <T> T dependencies(DependencyResolver dependencyResolver, Class<T> dependenciesInterface)
    {
        return (T) Proxy.newProxyInstance( dependenciesInterface.getClassLoader(), new Class[]{dependenciesInterface},
                new ProxyHandler(dependencyResolver) );
    }


    private static class ProxyHandler
            implements InvocationHandler
    {
        private DependencyResolver dependencyResolver;

        public ProxyHandler( DependencyResolver dependencyResolver )
        {
            this.dependencyResolver = dependencyResolver;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            try
            {
                if (method.getReturnType().equals( Supplier.class ))
                {
                    return dependencyResolver.provideDependency( (Class)((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0] );
                } else
                {
                    return dependencyResolver.resolveDependency( method.getReturnType() );
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw new UnsatisfiedDependencyException( e );
            }
        }
    }

}
