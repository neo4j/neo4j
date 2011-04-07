package org.neo4j.server.rest.repr;

import org.neo4j.server.rest.repr.ObjectRepresentation;

import com.tinkerpop.blueprints.pgm.Vertex;


public class VertexRepresentation extends ObjectRepresentation
{

    private final Vertex vertex;

    public VertexRepresentation( Vertex vertex )
    {
        super( RepresentationType.NODE );
        this.vertex = vertex;
    }

    @Mapping("self")
    public ValueRepresentation selfUri() {
        return ValueRepresentation.string( "vertex: " + vertex );
    }
}
