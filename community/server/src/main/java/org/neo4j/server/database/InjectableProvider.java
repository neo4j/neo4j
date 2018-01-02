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
package org.neo4j.server.database;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;

import javax.ws.rs.core.Context;

public abstract class InjectableProvider<E> extends AbstractHttpContextInjectable<E> implements
        com.sun.jersey.spi.inject.InjectableProvider<Context, Class<E>>
{
    public final Class<E> t;

    public static <E> InjectableProvider<? extends E> providerForSingleton(final E component, final Class<E> componentClass)
    {
        return new InjectableProvider<E>(componentClass) {
            @Override
            public E getValue(HttpContext httpContext) {
                return component;
            }
        };
    }

    public InjectableProvider( Class<E> t )
    {
        this.t = t;
    }

    public Injectable<E> getInjectable( ComponentContext ic, Context a, Class<E> c )
    {
        if ( c.equals( t ) )
        {
            return getInjectable();
        }

        return null;
    }

    public Injectable<E> getInjectable()
    {
        return this;
    }

    public ComponentScope getScope()
    {
        return ComponentScope.PerRequest;
    }
}
