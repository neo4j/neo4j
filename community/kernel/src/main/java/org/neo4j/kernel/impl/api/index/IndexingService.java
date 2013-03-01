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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.api.IndexNotFoundKernelException;
import org.neo4j.kernel.api.IndexState;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Manages the "schema indexes" that were introduced in 2.0. These indexes depend on the normal neo4j logical log
 * for transactionality. Each index has an {@link IndexRule}, which it uses to filter changes that come into the
 * database. Changes that apply to the the rule are indexed. This way, "normal" changes to the database can be
 * replayed to perform recovery after a crash.
 *
 * <h3>Recovery procedure</h3>
 *
 * Each index has a state, as defined in {@link org.neo4j.kernel.api.IndexState}, which is used during recovery. If
 * an index is anything but {@link org.neo4j.kernel.api.IndexState#ONLINE}, it will simply be destroyed and re-created.
 *
 * If, however, it is {@link org.neo4j.kernel.api.IndexState#ONLINE}, the index provider is required to also guarantee
 * that the index had been flushed to disk.
 */
public class IndexingService extends LifecycleAdapter
{
    // TODO create hierarchy of filters for smarter update processing

    private final JobScheduler scheduler;
    private final SchemaIndexProvider provider;

    private final ConcurrentHashMap<Long, IndexContext> indexes = new ConcurrentHashMap<Long, IndexContext>();
    private boolean online = false;

    public IndexingService( JobScheduler scheduler, SchemaIndexProvider provider )
    {
        this.scheduler = scheduler;
        this.provider = provider;

        if(provider == null)
        {
            // For now
            throw new IllegalStateException( "You cannot run the database without providing a schema index provider, " +
                    "please make sure that a valid provider is on your classpath." );
        }
    }

    // Recovery semantics: This is to be called after initIndexes, and after the database has run recovery.
    @Override
    public void start()
    {
        // Find all indexes that are not already online, and create them
        for ( IndexContext indexContext : indexes.values() )
        {
            if ( indexContext.getState() != IndexState.ONLINE )
            {
                indexContext.create();
            }
        }

        online = true;
    }

    @Override
    public void stop()
    {
        online = false;
    }

    public void update( Iterable<NodePropertyUpdate> updates ) {
        for (IndexContext context : indexes.values())
            context.update( updates );
    }

    public IndexContext getContextForRule( long indexId ) throws IndexNotFoundKernelException
    {
        IndexContext indexContext = indexes.get( indexId );
        if(indexContext == null)
        {
            throw new IndexNotFoundKernelException( "No index with id " + indexId + " exists." );
        }
        return indexContext;
    }

    /**
     * Called while the database starts up, before recovery.
     *
     * @param indexRules Known index rules before recovery.
     */
    public void initIndexes( Iterable<IndexRule> indexRules, NeoStore neoStore )
    {
        for ( IndexRule indexRule : indexRules )
        {
            long id = indexRule.getId();
            IndexState initialState = provider.getInitialState( id );
            System.out.println( "init " + indexRule + " => " + initialState );
            switch ( initialState )
            {
                case ONLINE:
                    indexes.put( id, createOnlineIndexContext( indexRule ) );
                    break;
                case POPULATING:
                case NON_EXISTENT:
                    // The database was shut down during population, or a crash has occurred, or some other
                    // sad thing.
                    indexes.put( id, createPopulatingIndexContext( indexRule, neoStore ) );
                    break;
            }
        }
    }

    /*
     * Creates a new index.
     *
     * This code is called from the transaction infrastructure during transaction commits, which means that
     * it is *vital* that it is stable, and handles errors very well. Failing here means that the entire db
     * will shut down.
     */
    public void createIndex( IndexRule rule, NeoStore neoStore )
    {
        IndexContext index = indexes.get( rule.getId() );
        if ( online )
        {
            assert index == null : "Index " + rule + " already exists";
            index = createPopulatingIndexContext( rule, neoStore );

            // Trigger the creation, only if the service is online. Otherwise,
            // creation will be triggered on start().
            index.create();
        }
        else if ( index == null )
        {
            index = createPopulatingIndexContext( rule, neoStore );
        }
        
        indexes.put( rule.getId(), index );
    }

    private IndexContext createOnlineIndexContext( IndexRule rule )
    {
        IndexContext result = new OnlineIndexContext( provider.getWriter( rule.getId() ) );
        result = new RuleUpdateFilterIndexContext( result, rule );
        result = new ServiceStateUpdatingIndexContext( rule, result );

        return result;
    }

    private IndexContext createPopulatingIndexContext( IndexRule rule, NeoStore neoStore )
    {
        final long ruleId = rule.getId();
        FlippableIndexContext flippableContext = new FlippableIndexContext( );

        // TODO: This is here because there is a circular dependency from PopulatingIndexContext to FlippableContext
        flippableContext.setFlipTarget( eager( new PopulatingIndexContext( scheduler, rule,
                provider.getPopulator( ruleId ), flippableContext, neoStore ) ) );
        flippableContext.flip();

        // Prepare for flipping to online mode
        flippableContext.setFlipTarget( new IndexContextFactory()
        {
            @Override
            public IndexContext create()
            {
                return new OnlineIndexContext( provider.getWriter( ruleId ) );
            }
        } );

        // TODO: Merge auto removing and rule updating?
        IndexContext result = new RuleUpdateFilterIndexContext( flippableContext, rule );
        result = new ServiceStateUpdatingIndexContext( rule, result );
        return result;
    }

    class ServiceStateUpdatingIndexContext extends DelegatingIndexContext {

        private final long ruleId;

        ServiceStateUpdatingIndexContext( IndexRule rule, IndexContext delegate )
        {
            super( delegate );
            this.ruleId = rule.getId();
        }

        @Override
        public void drop()
        {
            super.drop();
            indexes.remove( ruleId, this );
        }
    }

    public void flushAll()
    {
        for ( IndexContext context : indexes.values() )
        {
            context.force();
        }
    }

    private static IndexContextFactory eager( final IndexContext context )
    {
        return new IndexContextFactory()
        {
            @Override
            public IndexContext create()
            {
                return context;
            }
        };
    }
}
