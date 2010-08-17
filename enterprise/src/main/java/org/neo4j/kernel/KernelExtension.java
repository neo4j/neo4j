package org.neo4j.kernel;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;

public abstract class KernelExtension extends Service
{
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

    public static abstract class KernelData
    {
        private final String instanceId;

        KernelData( String instanceId )
        {
            this.instanceId = instanceId;
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

        void shutdown( Logger log )
        {
            for ( KernelExtension loaded : state.keySet() )
            {
                try
                {
                    loaded.unload( this );
                }
                catch ( Exception ex )
                {
                    log.warning( "Error unloading " + loaded + ": " + ex );
                }
            }
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
            if ( !result.isAssignableFrom( method.getReturnType() ) ) return null;
            if ( !Modifier.isPublic( method.getModifiers() ) ) return null;
        }
        catch ( Exception e )
        {
            return null;
        }
        return new Function<T>( result, kernel, method );
    }
}
