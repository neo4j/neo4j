package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class IntegratedIndexingService extends LifecycleAdapter implements IndexingService
{
    // TODO force indexes on shutdown/stop ?

    // TODO create hierachy of filters for smarter update processing

    private final ExecutorService executor;
    private final SchemaIndexProvider provider;
    private final NeoStore store;

    private final ConcurrentHashMap<Long, IndexContext> contexts = new ConcurrentHashMap<Long, IndexContext>();

    IntegratedIndexingService( ExecutorService executor, SchemaIndexProvider provider, NeoStore store )
    {
        this.executor = executor;
        this.provider = provider;
        this.store = store;
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates ) {
        for (IndexContext context : contexts.values())
            context.update( updates );
    }

    @Override
    public IndexContext getContextForRule( IndexRule rule )
    {
        long ruleId = rule.getId();
        IndexContext indexContext = contexts.get( ruleId );
        if (indexContext == null)
        {
            IndexContext potentialNewContext = createContextForRule( rule );
            IndexContext oldContext = contexts.putIfAbsent( ruleId, potentialNewContext );
            return oldContext == null ? potentialNewContext : oldContext;
        }
        else
            return indexContext;
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    private IndexContext createContextForRule( IndexRule rule )
    {
        long ruleId = rule.getId();
        FlippableIndexContext flippableContext = new FlippableIndexContext( );

        // TODO: This is here because there is a circular dependency from PopulatingIndexContext to FlippableContext
        flippableContext.setFlipTarget(
                new PopulatingIndexContext( executor,
                                            rule, provider.getPopulatingWriter( ruleId ), flippableContext, store )
        );
        flippableContext.flip();

        // Prepare for flipping to online mode
        flippableContext.setFlipTarget( new OnlineIndexContext( provider.getOnlineWriter( ruleId ) ) );

        IndexContext result = new RuleUpdateFilterIndexContext( flippableContext, rule );
        result = new AutoRemovingIndexContext( rule, result );
        return result;
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
