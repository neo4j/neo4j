package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalBranch;

enum PrimitiveTypeFetcher
{
    NODE
    {
        @Override
        long getId( TraversalBranch source )
        {
            return source.node().getId();
        }

        @Override
        boolean idEquals( TraversalBranch source, long idToCompare )
        {
            return getId( source ) == idToCompare;
        }
    },
    RELATIONSHIP
    {
        @Override
        long getId( TraversalBranch source )
        {
            return source.relationship().getId();
        }

        @Override
        boolean idEquals( TraversalBranch source, long idToCompare )
        {
            Relationship relationship = source.relationship();
            return relationship != null && relationship.getId() == idToCompare;
        }
    };
    abstract long getId( TraversalBranch source );

    abstract boolean idEquals( TraversalBranch source, long idToCompare );
}
