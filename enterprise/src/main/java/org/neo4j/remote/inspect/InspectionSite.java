/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.remote.inspect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteSite;

/**
 * A remote site that wraps another remote site for inspection purposes.
 * @author Tobias Ivarsson
 */
public final class InspectionSite implements RemoteSite
{
    private final RemoteSite site;
    private final Inspector inspector;
    private static final Map<Method, Method> inspectorMethods;
    static
    {
        Map<Method, Method> methods = new HashMap<Method, Method>();
        for ( Method method : RemoteConnection.class.getMethods() )
        {
            try
            {
                methods.put( method, Inspector.class.getMethod( method
                    .getName(), method.getParameterTypes() ) );
            }
            catch ( Exception e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        inspectorMethods = Collections.unmodifiableMap( methods );
    }

    /**
     * Create a new inspection site.
     * @param site
     *            the actual site to call through to.
     * @param inspector
     *            the inspector to receive the messages from the inspection
     *            site.
     */
    public InspectionSite( RemoteSite site, Inspector inspector )
    {
        this.site = site;
        this.inspector = inspector;
    }

    public RemoteConnection connect()
    {
        return debugConnect( site.connect() );
    }

    public RemoteConnection connect( String username, String password )
    {
        return debugConnect( site.connect( username, password ) );
    }

    private RemoteConnection debugConnect( final RemoteConnection connection )
    {
        return ( RemoteConnection ) Proxy.newProxyInstance(
            RemoteConnection.class.getClassLoader(),
            new Class[] { RemoteConnection.class }, new InvocationHandler()
            {
                public Object invoke( Object proxy, Method method, Object[] args )
                    throws Throwable
                {
                    DynamicCallback<?> call = callback( method, method
                        .getReturnType(), args );
                    try
                    {
                        Object result = method.invoke( connection, args );
                        call.success( result );
                        return result;
                    }
                    catch ( Throwable ex )
                    {
                        call.failure( ex );
                        throw ex;
                    }
                }
            } );
    }

    protected <T> DynamicCallback<T> callback( Method method, Class<T> type,
        Object[] arguments )
    {
        return new DynamicCallback<T>( method, type, arguments );
    }

    private class DynamicCallback<T>
    {
        private CallBack<T> callback;

        DynamicCallback( Method method, Class<T> type, Object[] arguments )
        {
            this.callback = invoke( method, type, arguments );
        }

        @SuppressWarnings( "unchecked" )
        void success( Object result )
        {
            if ( callback != null )
            {
                try
                {
                    callback.success( ( T ) result );
                }
                catch ( Throwable t )
                {
                    t.printStackTrace();
                }
            }
        }

        void failure( Throwable ex )
        {
            if ( callback != null )
            {
                try
                {
                    callback.failure( ex );
                }
                catch ( Throwable t )
                {
                    t.printStackTrace();
                }
            }
        }
    }

    private <T> CallBack<T> invoke( Method method, Class<T> returnType,
        Object[] arguments )
    {
        Method target = inspectorMethods.get( method );
        try
        {
            return ( CallBack<T> ) target.invoke( inspector, arguments );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return null;
        }
    }
}
