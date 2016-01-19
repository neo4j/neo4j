package org.neo4j.cypher.internal.codegen;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

public final class CompiledReadOperationsUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledReadOperationsUtils()
    {
    }

    public static RelationshipIterator nodeGetRelationships(ReadOperations readOperations, long nodeId, Direction direction)
    {
        try
        {
            return readOperations.nodeGetRelationships( nodeId, direction );
        }
        catch ( EntityNotFoundException e )
        {
            throw new org.neo4j.cypher.internal.frontend.v3_0.CypherExecutionException(
                    e.getUserMessage( new org.neo4j.kernel.api.StatementTokenNameLookup( readOperations ) ), e );
        }
    }

}
