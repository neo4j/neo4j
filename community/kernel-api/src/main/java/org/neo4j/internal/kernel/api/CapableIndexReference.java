package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.ValueGroup;

public interface CapableIndexReference extends IndexReference, IndexCapability
{
    CapableIndexReference NO_INDEX = new CapableIndexReference()
    {
        IndexReference noIndexReference = IndexReference.NO_INDEX;

        @Override
        public IndexOrderCapability[] order( ValueGroup... valueGroups )
        {
            return new IndexOrderCapability[0];
        }

        @Override
        public IndexValueCapability value( ValueGroup... valueGroups )
        {
            return IndexValueCapability.NO;
        }

        @Override
        public boolean isUnique()
        {
            return noIndexReference.isUnique();
        }

        @Override
        public int label()
        {
            return noIndexReference.label();
        }

        @Override
        public int[] properties()
        {
            return noIndexReference.properties();
        }
    };
}
