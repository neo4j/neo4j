package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.ExpansionSource;

enum PrimitiveTypeFetcher
{
    NODE
    {
        @Override
        long getId( ExpansionSource source )
        {
            return source.node().getId();
        }

        @Override
        boolean idEquals( ExpansionSource source, long idToCompare )
        {
            return getId( source ) == idToCompare;
        }
    },
    RELATIONSHIP
    {
        @Override
        long getId( ExpansionSource source )
        {
            return source.relationship().getId();
        }

        @Override
        boolean idEquals( ExpansionSource source, long idToCompare )
        {
            Relationship relationship = source.relationship();
            return relationship != null && relationship.getId() == idToCompare;
        }
    };
    abstract long getId( ExpansionSource source );

    abstract boolean idEquals( ExpansionSource source, long idToCompare );
}
