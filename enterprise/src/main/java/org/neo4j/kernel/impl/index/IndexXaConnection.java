package org.neo4j.kernel.impl.index;

import java.util.Map;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnectionHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;

public abstract class IndexXaConnection extends XaConnectionHelpImpl
{
    public IndexXaConnection( XaResourceManager xaRm )
    {
        super( xaRm );
    }

    public abstract void createIndex( Class<? extends PropertyContainer> entityType,
            String indexName, Map<String, String> config );
}
