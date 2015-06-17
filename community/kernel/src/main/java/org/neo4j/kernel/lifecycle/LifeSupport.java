/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.lifecycle;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Support class for handling collections of Lifecycle instances. Manages the transitions from one state to another.
 * <p>
 * To use this, first add instances to it that implement the Lifecycle interface. When lifecycle methods on this
 * class are called it will try to invoke the same methods on the registered instances.
 * <p>
 * Components that internally owns other components that has a lifecycle can use this to control them as well.
 */
public class LifeSupport
        implements Lifecycle
{
    private volatile List<LifecycleInstance> instances = new ArrayList<LifecycleInstance>();
    private volatile LifecycleStatus status = LifecycleStatus.NONE;
    private final List<LifecycleListener> listeners = new ArrayList<LifecycleListener>();
    private final StringLogger log;
    
    public LifeSupport()
    {
        this( StringLogger.SYSTEM_ERR );
    }
    
    public LifeSupport( StringLogger log )
    {
        this.log = log;
    }

    /**
     * Initialize all registered instances, transitioning from status NONE to STOPPED.
     * <p>
     * If transition fails, then it goes to STOPPED and then SHUTDOWN, so it cannot be restarted again.
     */
    @Override
    public synchronized void init()
            throws LifecycleException
    {
        if ( status == LifecycleStatus.NONE )
        {
            status = changedStatus( this, status, LifecycleStatus.INITIALIZING );
            for ( LifecycleInstance instance : instances )
            {
                try
                {
                    instance.init();
                }
                catch ( LifecycleException e )
                {
                    status = changedStatus( this, status, LifecycleStatus.STOPPED );
                    try
                    {
                        shutdown();
                    }
                    catch ( LifecycleException e1 )
                    {
                        throw causedBy( e1, e );
                    }
                    throw e;
                }
            }
            status = changedStatus( this, status, LifecycleStatus.STOPPED );
        }
    }

    /**
     * Start all registered instances, transitioning from STOPPED to STARTED.
     * <p>
     * If it was previously not initialized, it will be initialized first.
     * <p>
     * If any instance fails to start, the already started instances will be stopped, so
     * that the overall status is STOPPED.
     *
     * @throws LifecycleException
     */
    @Override
    public synchronized void start()
            throws LifecycleException
    {
        init();

        if ( status == LifecycleStatus.STOPPED )
        {
            status = changedStatus( this, status, LifecycleStatus.STARTING );
            for ( LifecycleInstance instance : instances )
            {
                try
                {
                    instance.start();
                }
                catch ( LifecycleException e )
                {
                    // TODO perhaps reconsider chaining of exceptions coming from LifeSupports?
                    status = changedStatus( this, status, LifecycleStatus.STARTED );
                    try
                    {
                        stop();
                    }
                    catch ( LifecycleException e1 )
                    {
                        throw causedBy( e1, e );
                    }
                    throw e;
                }
            }
            status = changedStatus( this, status, LifecycleStatus.STARTED );
        }
    }

    /**
     * Stop all registered instances, transitioning from STARTED to STOPPED.
     * <p>
     * If any instance fails to stop, the rest of the instances will still be stopped,
     * so that the overall status is STOPPED.
     */
    @Override
    public synchronized void stop()
            throws LifecycleException
    {
        if ( status == LifecycleStatus.STARTED )
        {
            status = changedStatus( this, status, LifecycleStatus.STOPPING );
            LifecycleException ex = null;
            for ( int i = instances.size() - 1; i >= 0; i-- )
            {
                LifecycleInstance lifecycleInstance = instances.get( i );
                try
                {
                    lifecycleInstance.stop();
                }
                catch ( LifecycleException e )
                {
                    ex = causedBy( e, ex );
                }
            }

            status = changedStatus( this, status, LifecycleStatus.STOPPED );

            if ( ex != null )
            {
                throw ex;
            }
        }
    }

    /**
     * Shutdown all registered instances, transitioning from either STARTED or STOPPED to SHUTDOWN.
     * <p>
     * If any instance fails to shutdown, the rest of the instances will still be shut down,
     * so that the overall status is SHUTDOWN.
     */
    @Override
    public synchronized void shutdown()
            throws LifecycleException
    {
        LifecycleException ex = null;
        try
        {
            stop();
        }
        catch ( LifecycleException e )
        {
            ex = e;
        }

        if ( status == LifecycleStatus.STOPPED )
        {
            status = changedStatus( this, status, LifecycleStatus.SHUTTING_DOWN );
            for ( int i = instances.size() - 1; i >= 0; i-- )
            {
                LifecycleInstance lifecycleInstance = instances.get( i );
                try
                {
                    lifecycleInstance.shutdown();
                }
                catch ( LifecycleException e )
                {
                    ex = causedBy( e, ex );
                }
            }

            status = changedStatus( this, status, LifecycleStatus.SHUTDOWN );

            if ( ex != null )
            {
                throw ex;
            }
        }
    }

    /**
     * Restart an individual instance. All instances "after" the instance will be stopped first,
     * so that they don't try to use it during the restart. A restart is effectively a stop followed
     * by a start.
     *
     * @throws LifecycleException       if any start or stop fails
     * @throws IllegalArgumentException if instance is not registered
     */
    public synchronized void restart( Lifecycle instance )
            throws LifecycleException, IllegalArgumentException
    {
        if ( status == LifecycleStatus.STARTED )
        {
            boolean foundRestartingInstance = false;
            List<LifecycleInstance> restartingInstances = new ArrayList<LifecycleInstance>();
            for ( LifecycleInstance lifecycleInstance : instances )
            {
                if ( lifecycleInstance.instance == instance )
                {
                    foundRestartingInstance = true;
                }

                if ( foundRestartingInstance )
                {
                    restartingInstances.add( lifecycleInstance );
                }
            }

            if ( !foundRestartingInstance )
            {
                throw new IllegalArgumentException( "Instance is not registered" );
            }

            // Stop instances
            status = changedStatus( this, status, LifecycleStatus.STOPPING );
            LifecycleException ex = null;
            for ( int i = restartingInstances.size() - 1; i >= 0; i-- )
            {
                LifecycleInstance lifecycleInstance = restartingInstances.get( i );
                try
                {
                    lifecycleInstance.stop();
                }
                catch ( LifecycleException e )
                {
                    ex = causedBy( e, ex );
                }
            }

            // Failed stop - stop the whole thing to be safe
            if ( ex != null )
            {
                status = changedStatus( this, status, LifecycleStatus.STARTED );
                try
                {
                    stop();
                    throw ex;
                }
                catch ( LifecycleException e )
                {
                    throw causedBy( e, ex );
                }
            }

            // Start instances
            try
            {
                for ( int i = 0; i < restartingInstances.size(); i++ )
                {
                    LifecycleInstance lifecycle = restartingInstances.get( i );
                    lifecycle.start();
                }
                status = changedStatus( this, status, LifecycleStatus.STARTED );
            }
            catch ( LifecycleException e )
            {
                // Failed restart - stop the whole thing to be safe
                status = changedStatus( this, status, LifecycleStatus.STARTED );
                try
                {
                    stop();
                    throw e;
                }
                catch ( LifecycleException e1 )
                {
                    throw causedBy( e1, e );
                }
            }
        }
    }

    /**
     * Add a new Lifecycle instance. It will immediately be transitioned
     * to the state of this LifeSupport.
     *
     * @param instance the Lifecycle instance to add
     * @param <T>      type of the instance
     * @return the instance itself
     * @throws LifecycleException if the instance could not be transitioned properly
     */
    public synchronized <T> T add( T instance )
            throws LifecycleException
    {
        if ( instance instanceof Lifecycle )
        {
            LifecycleInstance newInstance = new LifecycleInstance( (Lifecycle) instance );
            List<LifecycleInstance> tmp = new ArrayList<>( instances );
            tmp.add(newInstance);
            instances = tmp;
            bringToState( newInstance );
        }
        return instance;
    }

    public synchronized boolean remove( Object instance )
    {
        for ( int i = 0; i < instances.size(); i++ )
        {
            if ( instances.get( i ).isInstance( instance ) )
            {
                List<LifecycleInstance> tmp = new ArrayList<>( instances );
                LifecycleInstance lifecycleInstance = tmp.remove( i );
                lifecycleInstance.shutdown();
                instances = tmp;
                return true;
            }
        }
        return false;
    }


    public Iterable<Lifecycle> getLifecycleInstances()
    {
        return Iterables.map( new Function<LifecycleInstance, Lifecycle>()
        {
            @Override
            public Lifecycle apply( LifecycleInstance lifecycleInstance )
            {
                return lifecycleInstance.instance;
            }
        }, new ArrayList<>(instances) );
    }

    /**
     * Shutdown and throw away all the current instances. After
     * this you can add new instances. This method does not change
     * the status of the LifeSupport (i.e. if it was started it will remain started)
     */
    public synchronized void clear()
    {
        for ( LifecycleInstance instance : instances )
        {
            instance.shutdown();
        }
        instances = new ArrayList<>( );
    }

    public LifecycleStatus getStatus()
    {
        return status;
    }

    public synchronized void addLifecycleListener( LifecycleListener listener )
    {
        listeners.add( listener );
    }

    public synchronized void removeLifecycleListener( LifecycleListener listener )
    {
        listeners.remove( listener );
    }


    public synchronized void dump( StringLogger logger )
    {
        logger.logLongMessage( "Lifecycle status:" + status.name(), new Visitor<StringLogger.LineLogger,
                RuntimeException>()
        {
            @Override
            public boolean visit( StringLogger.LineLogger element )
            {
                for ( LifecycleInstance instance : instances )
                {
                    element.logLine( instance.toString() );
                }

                return true;
            }
        }, true
        );
    }

    private void bringToState( LifecycleInstance instance )
            throws LifecycleException
    {
        switch ( status )
        {
            case STARTED:
                instance.start();
                break;
            case STOPPED:
                instance.init();
                break;
            case SHUTDOWN:
                break;
        }
    }

    private LifecycleException causedBy( LifecycleException exception, LifecycleException chainedLifecycleException )
    {
        if ( chainedLifecycleException == null )
        {
            return exception;
        }

        log.error( "Lifecycle exception", exception );
        log.error( "Chained lifecycle exception", chainedLifecycleException );
        
        Throwable current = exception;
        while ( current.getCause() != null )
        {
            current = current.getCause();
        }

        current.initCause( chainedLifecycleException );
        return exception;
    }

    private LifecycleStatus changedStatus( Lifecycle instance,
                                           LifecycleStatus oldStatus,
                                           LifecycleStatus newStatus
    )
    {
        for ( LifecycleListener listener : listeners )
        {
            listener.notifyStatusChanged( instance, oldStatus, newStatus );
        }

        return newStatus;
    }

    public boolean isRunning()
    {
        return status == LifecycleStatus.STARTED;
    }

    private class LifecycleInstance
            implements Lifecycle
    {
        Lifecycle instance;
        LifecycleStatus currentStatus = LifecycleStatus.NONE;

        private LifecycleInstance( Lifecycle instance )
        {
            this.instance = instance;
        }

        @Override
        public void init()
                throws LifecycleException
        {
            if ( currentStatus == LifecycleStatus.NONE )
            {
                currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.INITIALIZING );
                try
                {
                    instance.init();
                    currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.STOPPED );
                }
                catch ( Throwable e )
                {
                    currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.NONE );
                    throw new LifecycleException( instance, LifecycleStatus.NONE, LifecycleStatus.STOPPED, e );
                }
            }
        }

        @Override
        public void start()
                throws LifecycleException
        {
            if ( currentStatus == LifecycleStatus.NONE )
            {
                init();
            }
            if ( currentStatus == LifecycleStatus.STOPPED )
            {
                currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.STARTING );
                try
                {
                    instance.start();
                    currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.STARTED );
                }
                catch ( Throwable e )
                {
                    currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.STOPPED );
                    throw new LifecycleException( instance, LifecycleStatus.STOPPED, LifecycleStatus.STARTED, e );
                }
            }
        }

        @Override
        public void stop()
                throws LifecycleException
        {
            if ( currentStatus == LifecycleStatus.STARTED )
            {
                currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.STOPPING );
                try
                {
                    instance.stop();
                }
                catch ( Throwable e )
                {
                    log.error( "Exception when stopping " + instance, e );
                    throw new LifecycleException( instance, LifecycleStatus.STARTED, LifecycleStatus.STOPPED, e );
                }
                finally
                {
                    currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.STOPPED );
                }
            }
        }

        @Override
        public void shutdown()
                throws LifecycleException
        {
            if ( currentStatus == LifecycleStatus.STARTED )
            {
                stop();
            }

            if ( currentStatus == LifecycleStatus.STOPPED )
            {
                currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.SHUTTING_DOWN );
                try
                {
                    instance.shutdown();
                }
                catch ( Throwable e )
                {
                    throw new LifecycleException( instance, LifecycleStatus.STOPPED, LifecycleStatus.SHUTTING_DOWN, e );
                }
                finally
                {
                    currentStatus = changedStatus( instance, currentStatus, LifecycleStatus.SHUTDOWN );
                }
            }
        }

        @Override
        public String toString()
        {
            return instance.toString() + ": " + currentStatus.name();
        }

        public boolean isInstance( Object instance )
        {
            return this.instance == instance;
        }
    }
}
