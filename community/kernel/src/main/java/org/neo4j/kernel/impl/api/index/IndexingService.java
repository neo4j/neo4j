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
import java.util.concurrent.ExecutorService;

import org.neo4j.kernel.api.IndexNotFoundKernelException;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class IndexingService extends LifecycleAdapter
{

    // TODO create hierachy of filters for smarter update processing

    private final ExecutorService executor;
    private final SchemaIndexProvider provider;

    private final ConcurrentHashMap<Long, IndexContext> contexts = new ConcurrentHashMap<Long, IndexContext>();

    public IndexingService( ExecutorService executor, SchemaIndexProvider provider )
    {
        this.executor = executor;
        this.provider = provider;

        if(provider == null)
        {
            // For now
            throw new IllegalStateException( "You cannot run the database without providing a schema index provider, " +
                    "please make sure that a valid provider is on your classpath." );
        }
    }

    @Override
    public void init()
    {
        // During initialization, this service should somehow load all indexes and set up an index cake for each index
    }

    public void update( Iterable<NodePropertyUpdate> updates ) {
        for (IndexContext context : contexts.values())
            context.update( updates );
    }

    public IndexContext getContextForRule( long indexId ) throws IndexNotFoundKernelException
    {
        IndexContext indexContext = contexts.get( indexId );
        if(indexContext == null)
        {
            throw new IndexNotFoundKernelException( "No index with id " + indexId + " exists." );
        }
        return indexContext;
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
        long ruleId = rule.getId();
        FlippableIndexContext flippableContext = new FlippableIndexContext( );

        // TODO: This is here because there is a circular dependency from PopulatingIndexContext to FlippableContext
        flippableContext.setFlipTarget(
                new PopulatingIndexContext( executor,
                        rule, provider.getPopulatingWriter( ruleId ), flippableContext, neoStore )
        );
        flippableContext.flip();

        // Prepare for flipping to online mode
        flippableContext.setFlipTarget( new OnlineIndexContext( provider.getOnlineWriter( ruleId ) ) );

        // TODO: Merge auto removing and rule updating?
        IndexContext result = new RuleUpdateFilterIndexContext( flippableContext, rule );
        result = new AutoRemovingIndexContext( rule, result );

        IndexContext preExisting = contexts.put( rule.getId(), result );
        assert preExisting == null;

        // Trigger the creation
        result.create();
    }

    class AutoRemovingIndexContext extends DelegatingIndexContext {

        private final long ruleId;

        AutoRemovingIndexContext( IndexRule rule, IndexContext delegate )
        {
            super( delegate );
            this.ruleId = rule.getId();
        }

        @Override
        public void drop()
        {
            super.drop();
            contexts.remove( ruleId, this );
        }
    }
}
