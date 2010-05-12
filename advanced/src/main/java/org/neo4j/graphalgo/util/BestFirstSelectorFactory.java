package org.neo4j.graphalgo.util;

import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.SourceSelectorFactory;

public class BestFirstSelectorFactory implements SourceSelectorFactory
{
    public SourceSelector create( ExpansionSource startSource )
    {
        return new BestFirstSelector( startSource );
    }

    private final class BestFirstSelector implements SourceSelector
    {
        private ExpansionSource source;

        BestFirstSelector( ExpansionSource source )
        {
            this.source = source;
        }

        public ExpansionSource nextPosition()
        {
            // TODO Auto-generated method stub
            return null;
        }
    }
}
