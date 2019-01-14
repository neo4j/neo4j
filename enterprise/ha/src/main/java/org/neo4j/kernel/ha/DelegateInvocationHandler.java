/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.LazySingleReference;

/**
 * InvocationHandler for dynamic proxies that delegate calls to a given backing implementation. This is mostly
 * used to present a single object to others, while being able to switch implementation at runtime.
 *
 * {@link #cement()}: acquire a proxy that will have its delegate assigned the next call to
 * {@link #setDelegate(Object)}. This is useful if one {@link DelegateInvocationHandler} depends on
 * another which will have its delegate set later than this one.
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
     *
     * @return the old delegate
     */
    public T setDelegate( T delegate )
    {
        T oldDelegate = this.delegate;
        this.delegate = delegate;
        harden();
        concrete.invalidate();
        return oldDelegate;
    }

    /**
     * Updates {@link #cement() cemented} delegates with the current delegate, making it concrete.
     * Callers of {@link #cement()} in between this call and the previous call to {@link #setDelegate(Object)}
     * will see the current delegate.
     */
    @SuppressWarnings( "unchecked" )
    void harden()
    {
        ((Concrete<T>) Proxy.getInvocationHandler( concrete.get() )).set( delegate );
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
    {
        if ( delegate == null )
        {
            throw new StateChangedTransactionFailureException(
                    "This transaction made assumptions about the instance it is executing " +
                    "on that no longer hold true. This normally happens when a transaction " +
                    "expects the instance it is executing on to be in some specific cluster role" +
                    "(such as 'master' or 'slave') and the instance " +
                    "changing state while the transaction is executing. Simply retry your " +
                    "transaction and you should see a successful outcome." );
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
        return concrete.get();
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
                throw new TransientDatabaseFailureException(
                        "Instance state is not valid. There is no master currently available. Possible causes " +
                                "include unavailability of a majority of the cluster members or network failure " +
                                "that caused this instance to be partitioned away from the cluster" );
            }

            return proxyInvoke( delegate, method, args );
        }

        @Override
        public String toString()
        {
            return "Concrete[" + delegate + "]";
        }
    }

    /**
     * Because we don't want the public API to implement `HasStatus`, and because
     * we don't want to change the API from throwing `TransactionFailureException` for
     * backwards compat reasons, we throw this sub-class that adds a status code.
     */
    static class StateChangedTransactionFailureException extends TransactionFailureException implements Status.HasStatus
    {
        StateChangedTransactionFailureException( String msg )
        {
            super( msg );
        }

        @Override
        public Status status()
        {
            return Status.Transaction.InstanceStateChanged;
        }
    }
}
