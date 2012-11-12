/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import static org.neo4j.helpers.Exceptions.launderedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.util.StringLogger;

public abstract class KernelData
{
    private static final Map<String, KernelData> instances = new HashMap<String, KernelData>();
    private static int ID_COUNTER = 0;

    private static synchronized String newInstance( KernelData instance )
    {
        final String instanceId = Integer.toString( ID_COUNTER++ );
        instances.put( instanceId, instance );
        return instanceId;
    }

    private static synchronized Collection<KernelData> kernels()
    {
        return new LinkedList<KernelData>( instances.values() );
    }

    private static synchronized void removeInstance( String instanceId )
    {
        instances.remove( instanceId );
    }

    static void visitAll( KernelExtension<?> extension, Object param )
    {
        for ( KernelData kernel : kernels() )
        {
            try
            {
                kernel.accept( extension, param );
            }
            catch ( Throwable cause )
            {
                System.err.println( "Agent visit failure: " + cause );
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    <S> void accept( KernelExtension<S> extension, Object param )
    {
        @SuppressWarnings( "hiding" ) Object state = this.state.get( extension );
        if ( state != null )
        {
            extension.agentVisit( this, (S) state, param );
        }
        else
        {
            state = extension.agentLoad( this, param );
            if ( state != null )
            {
                setState( extension, state );
            }
        }
    }

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

    public abstract Version version();

    public abstract Config getConfig();

    public abstract GraphDatabaseService graphDatabase();

    public abstract Map<Object, Object> getConfigParams();

    private final Map<KernelExtension<?>, Object> state = new HashMap<KernelExtension<?>, Object>();

    Collection<KernelExtension<?>> loadExtensionConfigurations( StringLogger msgLog )
    {
        Collection<KernelExtension<?>> loadedExtensions = new ArrayList<KernelExtension<?>>();
        for ( KernelExtension<?> extension : Service.load( KernelExtension.class ) )
        {
            try
            {
                extension.loadConfiguration( this );
                loadedExtensions.add( extension );
            }
            catch ( Throwable t )
            {
                msgLog.logMessage( "Failed to init extension " + extension, t, true );
            }
        }
        return loadedExtensions;
    }

    void loadIndexImplementations( IndexManagerImpl indexes, StringLogger msgLog )
    {
        for ( IndexProvider index : Service.load( IndexProvider.class ) )
        {
            try
            {
                indexes.addProvider( index.identifier(), index.load( this ) );
            }
            catch ( Throwable cause )
            {
                msgLog.logMessage( "Failed to load index provider " + index.identifier(), cause );
                if ( isAnUpgradeProblem( cause ) ) throw launderedException( cause );
                else cause.printStackTrace();
            }
        }
    }

    private boolean isAnUpgradeProblem( Throwable cause )
    {
        while ( cause != null )
        {
            if ( cause instanceof UpgradeNotAllowedByConfigurationException ) return true;
            cause = cause.getCause();
        }
        return false;
    }

    void loadExtensions( Collection<KernelExtension<?>> loadedExtensions, StringLogger msgLog )
    {
        for ( KernelExtension<?> extension : loadedExtensions )
        {
            try
            {
                @SuppressWarnings( "hiding" ) Object state = extension.load( this );
                if ( state != null )
                {
                    setState( extension, state );
                }
                msgLog.logMessage( "Extension " + extension + " loaded ok", true );
            }
            catch ( Throwable cause )
            {
                msgLog.logMessage( "Failed to load extension " + extension, cause, true );
            }
        }
    }

    synchronized void shutdown( StringLogger msgLog )
    {
        try
        {
            for ( Map.Entry<KernelExtension<?>, Object> loaded : state.entrySet() )
            {
                try
                {
                    unload( loaded.getKey(), loaded.getValue() );
                }
                catch ( Throwable cause )
                {
                    msgLog.logMessage( "Error unloading " + loaded, cause, true );
                }
            }
        }
        finally
        {
            state.clear();
            removeInstance( instanceId );
        }
    }

    @SuppressWarnings( "unchecked" )
    private <S> void unload( KernelExtension<S> extension, @SuppressWarnings( "hiding" ) Object state )
    {
        extension.unload( (S) state );
    }

    @SuppressWarnings( "unchecked" )
    final <S> S getState( KernelExtension<S> extension )
    {
        return (S) state.get( extension );
    }

    private Object setState( KernelExtension<?> extension, Object value )
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
        return getConfigParams().get( key );
    }
    
    public PropertyContainer properties()
    {
        return getConfig().getGraphDbModule().getNodeManager().getGraphProperties();
    }
}
