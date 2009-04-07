package org.neo4j.remote;

public class IndexSpecification implements EncodedObject
{
    private static final long serialVersionUID = 1L;
    final String identifier;

    public IndexSpecification( String identifier )
    {
        this.identifier = identifier;
    }
}
