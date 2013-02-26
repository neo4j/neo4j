/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public interface IndexingService extends Lifecycle
{
    void update( Iterable<NodePropertyUpdate> updates );

    IndexContext getContextForRule( IndexRule rule );

    public static class Adapter extends LifecycleAdapter implements IndexingService {
        public static final Adapter EMPTY = new Adapter();

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
        }

        @Override
        public IndexContext getContextForRule( IndexRule rule )
        {
            return IndexContext.Adapter.EMPTY;
        }
    }
}
