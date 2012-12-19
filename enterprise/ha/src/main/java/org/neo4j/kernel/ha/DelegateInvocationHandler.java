/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.graphdb.TransactionFailureException;

/**
 * InvocationHandler for dynamic proxies that delegate calls to a given backing implementation. This is mostly
 * used to present a single object to others, while being able to switch implementation at runtime.
 *
 * @param <T>
 */
public class DelegateInvocationHandler<T> implements InvocationHandler
{
    private volatile T delegate;

    public void setDelegate( T delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
    {
        if ( delegate == null )
        {
            throw new TransactionFailureException( "Instance state changed after this transaction started." );
        }
        try
        {
            return method.invoke( delegate, args );
        }
        catch ( InvocationTargetException e )
        {
            throw e.getCause();
        }
    }
    
    public static <T> T snapshot( T proxiedInstance )
    {
        @SuppressWarnings( "unchecked" )
        DelegateInvocationHandler<T> delegateHandler = (DelegateInvocationHandler<T>) Proxy.getInvocationHandler( proxiedInstance );
        return delegateHandler.delegate;
    }
}
