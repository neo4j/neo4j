/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.util.LazySingleReference;

/**
 * InvocationHandler for dynamic proxies that delegate calls to a given backing implementation. This is mostly
 * used to present a single object to others, while being able to switch implementation at runtime.
 * 
 * There are concepts of {@link #snapshot(Object)} and {@link #cement()} in here, which serves different purposes:
 * <ol>
 * <li>{@link #snapshot(Object)}: acquire the actual delegate at this particular point in time, pulling it out
 * from the proxy and using it directly. This is used for acquiring a snapshot and keep using that particular
 * instance, even if a new delegate is assigned for this handler.</li>
 * <li>{@link #cement()}: acquire a proxy that will have its delegate assigned the next call to
 * {@link #setDelegate(Object)}. This is useful if one {@link DelegateInvocationHandler} depends on
 * another which will have its delegate set later than this one.</li>
 * </ol>
 */
public class DelegateInvocationHandler<T> implements InvocationHandler
{
    private volatile T delegate;
    
    // A concrete version of delegate, where a user can request to cement this delegate so that it gets concrete
    // the next call to setDelegate and will never change since.
    private final LazySingleReference<T> concrete;
    
    public DelegateInvocationHandler( final Class<T> interfaceClass )
    {
        concrete = new LazySingleReference<T>()
        {
            @SuppressWarnings( "unchecked" )
            @Override
            protected T create()
            {
                return (T) Proxy.newProxyInstance( DelegateInvocationHandler.class.getClassLoader(),
                        new Class[] {interfaceClass}, new Concrete<>() );
            }
        };
    }

    /**
     * Updates the delegate for this handler, also {@link #harden() hardens} instances
     * {@link #cement() cemented} from the last call to {@link #setDelegate(Object)}.
     * This call will also dereference the {@link DelegateInvocationHandler.Concrete},
     * such that future calls to {@link #harden()} cannot affect any reference received
     * from {@link #cement()} prior to this call.
     * @param delegate the new delegate to set.
     */
    public void setDelegate( T delegate )
    {
        this.delegate = delegate;
        harden();
        concrete.invalidate();
    }

    /**
     * Updates {@link #cement() cemented} delegates with the current delegate, making it concrete.
     * Callers of {@link #cement()} in between this call and the previous call to {@link #setDelegate(Object)}
     * will see the current delegate.
     */
    @SuppressWarnings( "unchecked" )
    public void harden()
    {
        ((Concrete<T>)Proxy.getInvocationHandler( concrete.instance() )).set( delegate );
    }
    
    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
    {
        if ( delegate == null )
        {
            throw new TransactionFailureException( "Instance state changed after this transaction started." );
        }
        return proxyInvoke( delegate, method, args );
    }

    private static Object proxyInvoke( Object delegate, Method method, Object[] args )
            throws Throwable
    {
        try
        {
            return method.invoke( delegate, args );
        }
        catch ( InvocationTargetException e )
        {
            throw e.getCause();
        }
    }
    
    /**
     * Cements this delegate, i.e. returns an instance which will have its delegate assigned and hardened
     * later on so that it never will change after that point.
     */
    public T cement()
    {
        return concrete.instance();
    }
    
    /**
     * Takes a snapshot of the current delegate and returns that.
     */
    @SuppressWarnings( "unchecked" )
    public static <T> T snapshot( T proxiedInstance )
    {
        DelegateInvocationHandler<T> delegateHandler =
                (DelegateInvocationHandler<T>) Proxy.getInvocationHandler( proxiedInstance );
        return delegateHandler.delegate;
    }
    
    @Override
    public String toString()
    {
        return "Delegate[" + delegate + "]";
    }
    
    private static class Concrete<T> implements InvocationHandler
    {
        private volatile T delegate;
        
        void set( T delegate )
        {
            this.delegate = delegate;
        }
        
        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            if ( delegate == null )
            {
                throw new TransientDatabaseFailureException( "Transaction state is not valid. Perhaps a state change of the database has happened while this transaction was running?" );
            }
            
            return proxyInvoke( delegate, method, args );
        }
        
        @Override
        public String toString()
        {
            return "Concrete[" + delegate + "]";
        }
    }
}
