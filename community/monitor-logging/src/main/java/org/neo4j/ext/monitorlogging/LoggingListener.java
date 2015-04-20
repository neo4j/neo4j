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
package org.neo4j.ext.monitorlogging;

import java.lang.reflect.Method;
import java.util.Map;

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.MonitorListenerInvocationHandler;

public class LoggingListener implements MonitorListenerInvocationHandler
{

    private final Logging logging;
    private final Map<Class<?>, LogLevel> classes;

    public final Predicate<Method> predicate = new Predicate<Method>()
    {
        @Override
        public boolean accept( Method item )
        {
            Class<?> clazz = item.getDeclaringClass();
            return classes.containsKey( clazz );
        }
    };

    public LoggingListener( Logging logging, Map<Class<?>, LogLevel> classes )
    {
        assert (classes != null);
        this.classes = classes;
        this.logging = logging;
    }

    @Override
    public void invoke( Object proxy, Method method, Object[] args, String... tags ) throws Throwable
    {
        final Class<?> clazz = method.getDeclaringClass();
        final StringBuilder stringBuilder = new StringBuilder().append( method.getName() );
        formatArguments( stringBuilder, args, method.getParameterTypes() );
        classes.get( clazz ).log( logging.getMessagesLog( clazz ), stringBuilder.toString() );
    }

    private void formatArguments( StringBuilder stringBuilder, Object[] args, Class<?>[] types )
    {
        stringBuilder.append( "(" );

        for ( int i = 0; args != null && i < args.length; i++ )
        {
            (i > 0 ? stringBuilder.append( "," ) : stringBuilder)
                    .append( types[i].getSimpleName() )
                    .append( ":" )
                    .append( args[i] );
        }

        stringBuilder.append( ")" );
    }
}
