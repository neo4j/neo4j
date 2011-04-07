package org.neo4j.server.rest.repr;

import com.tinkerpop.blueprints.pgm.Edge;

public class EdgeRepresentation extends ObjectRepresentation {

	private final Edge edge;

    public EdgeRepresentation( Edge edge )
    {
        super( RepresentationType.NODE );
        this.edge = edge;
    }

    @Mapping("self")
    public ValueRepresentation selfUri() {
        return ValueRepresentation.string( "edge: " + edge );
    }

}
