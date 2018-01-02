/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.master;

import java.util.Collections;
import java.util.Set;

import org.neo4j.com.RequestContext;
import org.neo4j.function.Consumer;
import org.neo4j.function.Factory;
import org.neo4j.helpers.Clock;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.ConversationSPI;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.collection.ConcurrentAccessException;
import org.neo4j.kernel.impl.util.collection.NoSuchEntryException;
import org.neo4j.kernel.impl.util.collection.TimedRepository;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.util.JobScheduler.Groups.slaveLocksTimeout;

/**
 * Manages {@link Conversation} on master-side in HA.
 * It's expected to have one instance of {@link ConversationManager} on master.
 *
 * Used for keeping and monitoring clients {@link Conversation} on master side.
 */
public class ConversationManager extends LifecycleAdapter
{
    private static final int DEFAULT_TX_TIMEOUT_ADDITION = 5 * 1000;
    private static final int UNFINISHED_CONVERSATION_CLEANUP_DELAY = 1_000;

    private final int activityCheckIntervalMillis;
    private final int lockTimeoutAddition;
    private final Config config;
    private final ConversationSPI spi;
    private final Factory<Conversation> conversationFactory =  new Factory<Conversation>()
    {
        @Override
        public Conversation newInstance()
        {
            return new Conversation( spi.acquireClient() );
        }
    };

    TimedRepository<RequestContext,Conversation> conversations;
    private JobScheduler.JobHandle staleReaperJob;

    /**
     * Build conversation manager with default values for activity check interval and timeout addition.
     * @param spi - conversation manager spi
     * @param config - ha settings
     */
    public ConversationManager( ConversationSPI spi, Config config )
    {
        this( spi, config, UNFINISHED_CONVERSATION_CLEANUP_DELAY, DEFAULT_TX_TIMEOUT_ADDITION );
    }

    /**
     * Build conversation manager
     * @param spi - conversation manager spi
     * @param config - ha settings
     * @param activityCheckIntervalMillis - interval between conversations activity checking
     * @param lockTimeoutAddition - addition to read timeout used to build conversation timeout
     */
    public ConversationManager( ConversationSPI spi, Config config, int activityCheckIntervalMillis, int
            lockTimeoutAddition )
    {
        this.spi = spi;
        this.config = config;
        this.activityCheckIntervalMillis = activityCheckIntervalMillis;
        this.lockTimeoutAddition = lockTimeoutAddition;
    }

    @Override
    public void start()
    {
        conversations = createConversationStore();
        staleReaperJob = spi.scheduleRecurringJob( slaveLocksTimeout, activityCheckIntervalMillis,
                conversations );
    }

    @Override
    public void stop()
    {
        staleReaperJob.cancel( false );
        conversations = null;
    }

    public Conversation acquire( RequestContext context ) throws NoSuchEntryException, ConcurrentAccessException
    {
        return conversations.acquire( context );
    }

    public void release( RequestContext context )
    {
        conversations.release( context );
    }

    public void begin( RequestContext context ) throws ConcurrentAccessException
    {
        conversations.begin( context );
    }

    public void end( RequestContext context )
    {
        conversations.end( context );
    }

    public Set<RequestContext> getActiveContexts()
    {
        return conversations != null ? conversations.keys() : Collections.<RequestContext>emptySet() ;
    }

    /**
     * Stop conversation for specified context.
     * Conversation will still hold all already acquired locks, but will release all waiters and it will be
     * impossible to get new locks out of it.
     * @param context - context for which conversation should be stopped
     */
    public void stop( RequestContext context )
    {
        Conversation conversation = conversations.end( context );
        if ( conversation != null && conversation.isActive() )
        {
            conversation.stop();
        }
    }

    public Conversation acquire()
    {
        return getConversationFactory().newInstance();
    }

    protected TimedRepository<RequestContext,Conversation> createConversationStore()
    {
        return new TimedRepository<>( getConversationFactory(), getConversationReaper(),
                config.get( HaSettings.lock_read_timeout ) + lockTimeoutAddition, Clock.SYSTEM_CLOCK );
    }

    protected Consumer<Conversation> getConversationReaper()
    {
        return new Consumer<Conversation>()
        {
            @Override
            public void accept( Conversation conversation )
            {
                conversation.close();
            }
        };
    }

    protected Factory<Conversation> getConversationFactory()
    {
        return conversationFactory;
    }
}
