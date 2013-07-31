/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.logging;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * This class is here since kernel has a weak dependency on logback, i.e. it will be used if available,
 * otherwise a fallback will transparently be used in its place. So even if some other classes this class
 * may refer to are available at compile time, it cannot be written in a way that assumes them being there.
 * For example it cannot import those classes, but should resolve them by reflection instead.
 */
public class LogbackWeakDependency
{
    public static final Function<Config, Logging> DEFAULT_TO_CLASSIC = new Function<Config, Logging>()
    {
        @Override
        public Logging apply( Config config )
        {
            return new ClassicLoggingService( config );
        }
    };
    public static final Function<Config, Object> NEW_LOGGER_CONTEXT = new Function<Config, Object>()
    {
        @Override
        public Object apply( Config from )
        {
            try
            {
                return Class.forName( LOGGER_CONTEXT_CLASS_NAME ).getConstructor().newInstance();
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
        }
    };
    public static final Function<Config, Object> STATIC_LOGGER_CONTEXT = new Function<Config, Object>()
    {
        @Override
        public Object apply( Config config )
        {
            try
            {
                Class<?> loggerBinderClass = Class.forName( LOGGER_BINDER_CLASS_NAME );
                Object loggerBinder = loggerBinderClass.getDeclaredMethod( "getSingleton" ).invoke( null );
                return loggerBinder.getClass().getDeclaredMethod( "getLoggerFactory" ).invoke( loggerBinder );
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
        }
    };
    private static final String LOGGER_CONTEXT_CLASS_NAME = "ch.qos.logback.classic.LoggerContext";
    private static final String LOGGER_BINDER_CLASS_NAME = "org.slf4j.impl.StaticLoggerBinder";

    public Logging tryLoadLogbackService( Config config, Function<Config, Object> loggerContextGetter,
            Function<Config, Logging> otherwiseDefaultTo )
    {
        try
        {
            if ( logbackIsOnClasspath() )
            {
                return newLogbackService( config, loggerContextGetter.apply( config ) );
            }
        }
        catch ( Exception e )
        {   // OK, we'll return fallback below
        }
        return otherwiseDefaultTo.apply( config );
    }
    
    public Logging tryLoadLogbackService( Config config, Function<Config, Logging> otherwiseDefaultTo )
    {
        return tryLoadLogbackService( config, STATIC_LOGGER_CONTEXT, otherwiseDefaultTo );
    }

    /**
     * To work around the problem where we have an object that is actually a LoggerContext, but we
     * cannot refer to it as such since that class may not exist at runtime.
     */
    private Logging newLogbackService( Config config, Object loggerContext ) throws Exception
    {
        Class<?> loggerContextClass = Class.forName( LOGGER_CONTEXT_CLASS_NAME );
        return LogbackService.class
                .getConstructor( Config.class, loggerContextClass )
                .newInstance( config, loggerContext );
    }

    private boolean logbackIsOnClasspath()
    {
        try
        {
            getClass().getClassLoader().loadClass( LOGGER_CONTEXT_CLASS_NAME );
            return true;
        }
        catch ( ClassNotFoundException e )
        {
            return false;
        }
    }
}
