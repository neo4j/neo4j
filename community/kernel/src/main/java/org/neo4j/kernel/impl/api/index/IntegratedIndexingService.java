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
        IndexContext populatingWriter = new WritingIndexContext( provider.getPopulatingWriter( ruleId ), rule );
        PopulatingIndexContext populatingContext = new PopulatingIndexContext( populatingWriter, executor, store );
        IndexContext onlineWriter = new WritingIndexContext( provider.getOnlineWriter( ruleId ), rule );
        AtomicDelegatingIndexContext atomicContext = new AtomicDelegatingIndexContext( populatingContext );
        Flipper flipper = new Flipper( atomicContext, onlineWriter );
        populatingContext.setFlipper( flipper );
        IndexContext filteringContext = new RuleUpdateFilterIndexContext( atomicContext, rule );
        IndexContext autoRemovingContext = new AutoRemovingIndexContext( filteringContext );
        return autoRemovingContext;
    }

    class AutoRemovingIndexContext extends DelegatingIndexContext {
        AutoRemovingIndexContext( IndexContext delegate )
        {
            super( delegate );
        }

        @Override
        public void drop()
        {
            super.drop();
            contexts.remove( getIndexRule().getId(), this );
        }
    }
}
