package org.neo4j.server.rest.domain;

import org.neo4j.graphdb.Path;
import org.neo4j.server.rest.repr.RepresentationType;

import java.net.URI;

public enum TraverserReturnType
{
    node( RepresentationType.NODE )
            {
                @Override
                public Representation toRepresentation( URI baseUri, Path position )
                {
                    return new NodeRepresentation( baseUri, position.endNode() );
                }
            },
    relationship( RepresentationType.RELATIONSHIP )
            {
                @Override
                public Representation toRepresentation( URI baseUri, Path position )
                {
                    return new RelationshipRepresentation( baseUri, position.lastRelationship() );
                }
            },
    path( RepresentationType.PATH )
            {
                @Override
                public Representation toRepresentation( URI baseUri, Path position )
                {
                    return new PathRepresentation( baseUri, position );
                }
            };
    public final RepresentationType repType;

    private TraverserReturnType( RepresentationType repType )
    {
        this.repType = repType;
    }

    abstract Representation toRepresentation( URI baseUri, Path position );
}
