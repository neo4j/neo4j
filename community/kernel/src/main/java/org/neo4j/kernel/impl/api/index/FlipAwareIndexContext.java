package org.neo4j.kernel.impl.api.index;

public interface FlipAwareIndexContext extends IndexContext
{
    void setFlipper( Flipper flipper );
}
