package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Position;

/**
 * Hmm this is merely just another layer around {@link ExpansionSourceImpl},
 * optimize (maybe ugly, but still) by having ExpansionSource implement
 * {@link Position}?
 */
class PositionImpl implements Position
{
    private final ExpansionSourceImpl source;
    private Path path;

    PositionImpl( ExpansionSourceImpl source )
    {
        this.source = source;
    }

    public boolean atStartNode()
    {
        return source.relationship() == null;
    }

    public int depth()
    {
        return source.depth();
    }

    public Relationship lastRelationship()
    {
        return source.relationship();
    }

    public Node node()
    {
        return source.node();
    }

    public Path path()
    {
        // Instantiate path lazily since it contains a loop (which builds the
        // path from ExpansionSource objects)
        if ( this.path == null )
        {
            this.path = new TraversalPath( this.source );
        }
        return this.path;
    }
}
