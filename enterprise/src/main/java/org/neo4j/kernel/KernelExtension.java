/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.util.StringLogger;

public abstract class KernelExtension extends Service
{
    private static final String INSTANCE_ID = "instanceId";

    protected KernelExtension( String key )
    {
        super( key );
    }

    @Override
    public final int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public final boolean equals( Object obj )
    {
        return this.getClass().equals( obj.getClass() );
    }

    public final void agentLoad( String agentArgs )
    {
        final Map<String, String> parameters = new HashMap<String, String>();
        for ( String arg : agentArgs.split( ";" ) )
        {
            String[] parts = arg.split( "=", 2 );
            if ( parts.length == 2 )
            {
                arg = parts[0].trim();
                if ( INSTANCE_ID.equalsIgnoreCase( arg ) ) arg = INSTANCE_ID;
                parameters.put( arg, parts[1] );
            }
            else
            {
                parameters.put( arg.trim(), null );
            }
        }
        final KernelData kernel = KernelData.getInstance( parameters );
        if ( kernel == null ) throw new IllegalStateException( "could not load kernel" );
        kernel.extraParameters.putAll( parameters );
        this.load( kernel );
    }

    public static abstract class KernelData
    {
        private static final Map<String, KernelData> instances = new HashMap<String, KernelData>();
        private static int ID_COUNTER = 0;

        private static synchronized String newInstance( KernelData instance )
        {
            final String instanceId = Integer.toString( ID_COUNTER++ );
            instances.put( instanceId, instance );
            return instanceId;
        }

        private static synchronized KernelData getInstance( Map<String, String> parameters )
        {
            String instanceId = parameters.remove( INSTANCE_ID );
            if ( instanceId != null ) return instances.get( instanceId );
            if ( instances.size() == 1 ) return instances.values().iterator().next();
            return null;
        }

        private static synchronized void removeInstance( String instanceId )
        {
            instances.remove( instanceId );
        }

        private final Map<String, String> extraParameters = new HashMap<String, String>();
        private final String instanceId;

        KernelData()
        {
            instanceId = newInstance( this );
        }

        public final String instanceId()
        {
            return instanceId;
        }

        @Override
        public final int hashCode()
        {
            return instanceId.hashCode();
        }

        @Override
        public final boolean equals( Object obj )
        {
            return obj instanceof KernelData && instanceId.equals( ( (KernelData) obj ).instanceId );
        }

        public abstract String version();

        public abstract Config getConfig();

        public abstract GraphDatabaseService graphDatabase();

        public abstract Map<Object, Object> getConfigParams();

        private final Map<KernelExtension, Object> state = new HashMap<KernelExtension, Object>();

        void startup( StringLogger msgLog )
        {
            for ( KernelExtension extension : Service.load( KernelExtension.class ) )
            {
                try
                {
                    extension.load( this );
                    msgLog.logMessage( "Extension " + extension + " loaded ok" );
                }
                catch ( Exception ex )
                {
                    msgLog.logMessage( "Failed to load extension " + extension, ex );
                }
            }
        }

        synchronized void shutdown( StringLogger msgLog )
        {
            for ( KernelExtension loaded : state.keySet() )
            {
                try
                {
                    loaded.unload( this );
                }
                catch ( Exception ex )
                {
                    msgLog.logMessage( "Error unloading " + loaded, ex );
                }
            }
            removeInstance( instanceId );
        }

        public final Object getState( KernelExtension extension )
        {
            return state.get( extension );
        }

        public final Object setState( KernelExtension extension, Object value )
        {
            if ( value == null )
            {
                return state.remove( extension );
            }
            else
            {
                return state.put( extension, value );
            }
        }

        public final Object getParam( String key )
        {
            if ( extraParameters.containsKey( key ) )
            {
                return extraParameters.get( key );
            }
            else
            {
                return getConfigParams().get( key );
            }
        }
    }

    /**
     * Load this extension for a particular Neo4j Kernel.
     */
    protected abstract void load( KernelData kernel );

    protected void unload( KernelData kernel )
    {
        // Default: do nothing
    }

    protected boolean isLoaded( KernelData kernel )
    {
        return kernel.getState( this ) != null;
    }

    public class Function<T>
    {
        private final Class<T> type;
        private final KernelData kernel;
        private final Method method;

        private Function( Class<T> type, KernelData kernel, Method method )
        {
            this.type = type;
            this.kernel = kernel;
            this.method = method;
        }

        public T call( Object... args )
        {
            Object[] arguments = new Object[args == null ? 1 : ( args.length + 1 )];
            arguments[0] = kernel;
            if ( args != null && args.length > 0 )
            {
                System.arraycopy( args, 0, arguments, 1, args.length );
            }
            try
            {
                return type.cast( method.invoke( KernelExtension.this, arguments ) );
            }
            catch ( IllegalAccessException e )
            {
                throw new IllegalStateException( "Access denied", e );
            }
            catch ( InvocationTargetException e )
            {
                Throwable exception = e.getTargetException();
                if ( exception instanceof RuntimeException )
                {
                    throw (RuntimeException) exception;
                }
                else if ( exception instanceof Error )
                {
                    throw (Error) exception;
                }
                else
                {
                    throw new RuntimeException( "Unexpected exception: " + exception.getClass(),
                            exception );
                }
            }
        }
    }

    protected <T> Function<T> function( KernelData kernel, String name, Class<T> result,
            Class<?>... params )
    {
        Class<?>[] parameters = new Class[params == null ? 1 : ( params.length + 1 )];
        parameters[0] = KernelData.class;
        if ( params != null && params.length != 0 )
        {
            System.arraycopy( params, 0, parameters, 1, params.length );
        }
        final Method method;
        try
        {
            method = getClass().getMethod( name, parameters );
            /* if ( !result.isAssignableFrom( method.getReturnType() ) ) return null; */
            if ( !Modifier.isPublic( method.getModifiers() ) ) return null;
        }
        catch ( Exception e )
        {
            return null;
        }
        return new Function<T>( result, kernel, method );
    }
}
