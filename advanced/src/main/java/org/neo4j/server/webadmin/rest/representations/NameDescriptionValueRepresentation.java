package org.neo4j.server.webadmin.rest.representations;

import org.neo4j.server.rest.repr.ObjectRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.ValueRepresentation;

public class NameDescriptionValueRepresentation extends ObjectRepresentation
{
    private String name;
    private String description;
    private Representation value;

    public NameDescriptionValueRepresentation(String name, String description, Representation value)
    {
        super( "nameDescriptionValue" );
        this.name = name;
        this.description = description;
        this.value = value;
    }

    @Mapping( "name" )
    public ValueRepresentation getName()
    {
        return ValueRepresentation.string( name );
    }

    @Mapping( "description" )
    public ValueRepresentation getDescription()
    {
        return ValueRepresentation.string( description );
    }

    @Mapping( "value" )
    public Representation getValue()
    {
        return value;
    }

}
