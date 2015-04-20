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
package org.neo4j.test.ha;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * This retry rule allows tests that throws exceptions to be tried again if during that test there was >5 second GC.
 *
 * Most commonly used with cluster tests where GC's cause master switches during the test.
 *
 * Make sure to put this rule at the bottom of all rule fields, so that it runs outermost.
 */
public class RetryOnGcRule implements TestRule
{
    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                final AtomicLong periodGcDuration = new AtomicLong();
                NotificationListener listener = new NotificationListener()
                {
                    public void handleNotification( Notification notification, Object handback )
                    {
                        if ( notification.getType().equals( GarbageCollectionNotificationInfo
                                .GARBAGE_COLLECTION_NOTIFICATION ) )
                        {
                            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
                                    (CompositeData) notification.getUserData() );
                            long duration = info.getGcInfo().getDuration();
                            periodGcDuration.addAndGet( duration );
                        }
                    }
                };

                List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
                for ( GarbageCollectorMXBean gcbean : gcbeans )
                {
                    NotificationEmitter emitter = (NotificationEmitter) gcbean;
                    emitter.addNotificationListener( listener, null, null );
                }

                try
                {
                    Throwable e = null;

                    for ( int i = 0; i < 5; i++ )
                    {
                        try
                        {
                            base.evaluate();
                            return;
                        }
                        catch ( Throwable ex )
                        {
                            e = ex;
                            ex.printStackTrace();

                            // Check GC duration
                            if ( periodGcDuration.get() < TimeUnit.SECONDS.toMillis( 5 ) )
                            {
                                // Don't try again
                                throw ex;
                            }
                        }
                        periodGcDuration.set( 0 );
                    }

                    throw e;
                } finally
                {
                    for ( GarbageCollectorMXBean gcbean : gcbeans )
                    {
                        NotificationEmitter emitter = (NotificationEmitter) gcbean;
                        emitter.removeNotificationListener( listener, null, null );
                    }
                }
            }
        };
    }
}
