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
package org.neo4j.jmx.impl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.lang.reflect.Proxy.newProxyInstance;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.util.Preconditions.requirePositive;

/**
 * The purpose of this proxy is to take a snapshot of all MBean attributes and return those cached values to prevent excessive resource consumption
 * in case of frequent calls and expensive attribute calculations. Snapshot is updated no earlier than {@link #updateInterval} ms after previous update.
 */
class ThrottlingBeanSnapshotProxy implements InvocationHandler
{
    private final Set<Method> getters;
    private final Object target;
    private final Clock clock;
    private final Object lock = new Object();
    private final long updateInterval;
    private long lastUpdateTime;
    private Map<Method, ?> lastSnapshot;

    private <T> ThrottlingBeanSnapshotProxy( Class<T> iface, T target, long updateInterval, Clock clock )
    {
        this.getters = stream( iface.getDeclaredMethods() )
                .filter( m -> m.getReturnType() != Void.TYPE )
                .filter( m -> m.getParameterCount() == 0 )
                .collect( toSet() );
        this.target = target;
        this.updateInterval = updateInterval;
        this.clock = clock;
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws IllegalAccessException, InvocationTargetException
    {
        if ( !getters.contains( method ) )
        {
            return method.invoke( target, args );
        }
        synchronized ( lock )
        {
            final long now = clock.millis();
            final long age = now - lastUpdateTime;
            if ( lastSnapshot == null || age >= updateInterval )
            {
                lastUpdateTime = now;
                lastSnapshot = takeSnapshot();
            }
            return lastSnapshot.get( method );
        }
    }

    private Map<Method, ?> takeSnapshot() throws InvocationTargetException, IllegalAccessException
    {
        final Map<Method, Object> snapshot = new HashMap<>();
        for ( Method getter : getters )
        {
            final Object value = getter.invoke( target );
            snapshot.put( getter, value );
        }
        return snapshot;
    }

    static <I, T extends I> I newThrottlingBeanSnapshotProxy( Class<I> iface, T target, long updateInterval, Clock clock )
    {
        if ( updateInterval == 0 )
        {
            return target;
        }
        if ( !iface.isInterface() )
        {
            throw new IllegalArgumentException( iface + " is not an interface" );
        }
        requirePositive( updateInterval );
        final ThrottlingBeanSnapshotProxy proxy = new ThrottlingBeanSnapshotProxy( iface, target, updateInterval, clock );
        return iface.cast( newProxyInstance( iface.getClassLoader(), new Class[] {iface}, proxy ) );
    }
}
