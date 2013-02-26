package org.neo4j.kernel.impl.api.index;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class RuleUpdateFilterIndexContext extends DelegatingIndexContext<IndexContext>
{
    private final IndexRule rule;

    private final Predicate<NodePropertyUpdate> ruleMatchingUpdates = new Predicate<NodePropertyUpdate>()
    {
        @Override
        public boolean accept( NodePropertyUpdate item )
        {
            return item.getPropertyKeyId() == rule.getPropertyKey() && item.hasLabel( rule.getLabel() );
        }
    };

    public RuleUpdateFilterIndexContext( IndexContext delegate, IndexRule rule )
    {
        super( delegate );
        this.rule = rule;
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        super.update( new FilteringIterable<NodePropertyUpdate>( updates, ruleMatchingUpdates ) );
    }
}
