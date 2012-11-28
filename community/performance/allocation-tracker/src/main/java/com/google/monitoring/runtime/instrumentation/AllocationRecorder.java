/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package com.google.monitoring.runtime.instrumentation;

import java.lang.instrument.Instrumentation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.MapMaker;

/**
 * The logic for recording allocations, called from bytecode rewritten by
 * {@link AllocationInstrumenter}.
 *
 * @author jeremymanson@google.com (Jeremy Manson)
 * @author fischman@google.com (Ami Fischman)
 */
public class AllocationRecorder
{
    static
    {
        // Sun's JVMs in 1.5.0_06 and 1.6.0{,_01} have a bug where calling
        // Instrumentation.getObjectSize() during JVM shutdown triggers a
        // JVM-crashing assert in JPLISAgent.c, so we make sure to not call it after
        // shutdown.  There can still be a race here, depending on the extent of the
        // JVM bug, but this seems to be good enough.
        // instrumentation is volatile to make sure the threads reading it (in
        // recordAllocation()) see the updated value; we could do more
        // synchronization but it's not clear that it'd be worth it, given the
        // ambiguity of the bug we're working around in the first place.
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                setInstrumentation( null );
            }
        } );
    }

    // See the comment above the addShutdownHook in the static block above
    // for why this is volatile.
    private static volatile Instrumentation instrumentation = null;

    static Instrumentation getInstrumentation()
    {
        return instrumentation;
    }

    static void setInstrumentation( Instrumentation inst )
    {
        instrumentation = inst;
    }

    // Mostly because, yes, arrays are faster than collections.
    private static volatile Sampler[] additionalSamplers;

    // Protects mutations of additionalSamplers.  Reads are okay because
    // the field is volatile, so anyone who reads additionalSamplers
    // will get a consistent view of it.
    private static final Object samplerLock = new Object();

    // List of packages that can add samplers.
    private static final List<String> classNames = new ArrayList<String>();

    static
    {
        classNames.add( "com.google.monitoring.runtime." );
    }

    // Used for reentrancy checks
    private static final ThreadLocal<Boolean> recordingAllocation = new ThreadLocal<Boolean>();

    // Stores the object sizes for the last ~100000 encountered classes
    private static final ForwardingMap<Class<?>, Long> classSizesMap =
            new ForwardingMap<Class<?>, Long>()
            {
                private final ConcurrentMap<Class<?>, Long> map = new MapMaker()
                        .weakKeys()
                        .makeMap();

                @Override
                public Map<Class<?>, Long> delegate()
                {
                    return map;
                }

                // The approximate maximum size of the map
                private static final int MAX_SIZE = 100000;

                // The approximate current size of the map; since this is not an AtomicInteger
                // and since we do not synchronize the updates to this field, it will only be
                // an approximate size of the map; it's good enough for our purposes though,
                // and not synchronizing the updates saves us some time
                private int approximateSize = 0;

                @Override
                public Long put( Class<?> key, Long value )
                {
                    // if we have too many elements, delete about 10% of them
                    // this is expensive, but needs to be done to keep the map bounded
                    // we also need to randomize the elements we delete: if we remove the same
                    // elements all the time, we might end up adding them back to the map
                    // immediately after, and then remove them again, then add them back, etc.
                    // which will cause this expensive code to be executed too often
                    if ( approximateSize >= MAX_SIZE )
                    {
                        for ( Iterator<Class<?>> it = keySet().iterator(); it.hasNext(); )
                        {
                            it.next();
                            if ( Math.random() < 0.1 )
                            {
                                it.remove();
                            }
                        }

                        // get the exact size; another expensive call, but we need to correct
                        // approximateSize every once in a while, or the difference between
                        // approximateSize and the actual size might become significant over time;
                        // the other solution is synchronizing every time we update approximateSize,
                        // which seems even more expensive
                        approximateSize = size();
                    }

                    approximateSize++;
                    return super.put( key, value );
                }
            };

    /**
     * Adds a {@link Sampler} that will get run <b>every time an allocation is
     * performed from Java code</b>.  Use this with <b>extreme</b> judiciousness!
     *
     * @param sampler The sampler to add.
     */
    public static void addSampler( Sampler sampler )
    {
        synchronized ( samplerLock )
        {
            Sampler[] samplers = additionalSamplers;
            /* create a new list of samplers from the old, adding this sampler */
            if ( samplers != null )
            {
                Sampler[] newSamplers = new Sampler[samplers.length + 1];
                System.arraycopy( samplers, 0, newSamplers, 0, samplers.length );
                newSamplers[samplers.length] = sampler;
                additionalSamplers = newSamplers;
            }
            else
            {
                Sampler[] newSamplers = new Sampler[1];
                newSamplers[0] = sampler;
                additionalSamplers = newSamplers;
            }
        }
    }

    /**
     * Returns the size of the given object. If the object is not an array, we
     * check the cache first, and update it as necessary.
     *
     * @param obj     the object.
     * @param isArray indicates if the given object is an array.
     * @return the size of the given object.
     */
    private static long getObjectSize( Object obj, boolean isArray )
    {
        if ( isArray )
        {
            return instrumentation.getObjectSize( obj );
        }

        Class<?> clazz = obj.getClass();
        Long classSize = classSizesMap.get( clazz );
        if ( classSize == null )
        {
            classSize = instrumentation.getObjectSize( obj );
            classSizesMap.put( clazz, classSize );
        }

        return classSize;
    }

    public static void recordAllocation( Class<?> cls, Object newObj )
    {
        // The use of replace makes calls to this method relatively ridiculously
        // expensive.
        String typename = cls.getName().replace( ".", "/" );
        recordAllocation( -1, typename, newObj );
    }

    /**
     * Records the allocation.  This method is invoked on every allocation
     * performed by the system.
     *
     * @param count  the count of how many instances are being
     *               allocated, if an array is being allocated.  If an array is not being
     *               allocated, then this value will be -1.
     * @param desc   the descriptor of the class/primitive type
     *               being allocated.
     * @param newObj the new <code>Object</code> whose allocation is being
     *               recorded.
     */
    public static void recordAllocation( int count, String desc, Object newObj )
    {
        if ( recordingAllocation.get() == Boolean.TRUE )
        {
            return;
        }
        else
        {
            recordingAllocation.set( Boolean.TRUE );
        }

        // NB: This could be smaller if the defaultSampler were merged with the
        // optional samplers.  However, you don't need the optional samplers in
        // the common case, so I thought I'd save some space.
        if ( instrumentation != null )
        {
            // calling getObjectSize() could be expensive,
            // so make sure we do it only once per object
            long objectSize = -1;

            Sampler[] samplers = additionalSamplers;
            if ( samplers != null )
            {
                if ( objectSize < 0 )
                {
                    objectSize = getObjectSize( newObj, (count >= 0) );
                }
                for ( Sampler sampler : samplers )
                {
                    sampler.sampleAllocation( count, desc, newObj, objectSize );
                }
            }
        }

        recordingAllocation.set( Boolean.FALSE );
    }

    /**
     * Helper method to force recording; for unit tests only.
     */
    public static void recordAllocationForceForTest( int count, String desc,
                                                     Object newObj )
    {
        // Make sure we get the right number of elided frames
        recordAllocationForceForTestReal( count, desc, newObj, 2 );
    }

    public static void recordAllocationForceForTestReal(
            int count, String desc, Object newObj, int recurse )
    {
        if ( recurse != 0 )
        {
            recordAllocationForceForTestReal( count, desc, newObj, recurse - 1 );
            return;
        }
    }
}
