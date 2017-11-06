package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.ValueGroup;

/**
 * Reference to a specific index together with it's capabilities. This reference is valid until the schema of the database changes
 * (that is a create/drop of an index or constraint occurs).
 */
public interface CapableIndexReference extends IndexReference, IndexCapability
{
    CapableIndexReference NO_INDEX = new CapableIndexReference()
    {
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
            return false;
        }

        @Override
        public int label()
        {
            return Token.NO_TOKEN;
        }

        @Override
        public int[] properties()
        {
            return new int[0];
        }
    };
}
