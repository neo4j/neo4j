package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public final class IndexedEntityRepresentation extends MappingRepresentation implements
        ExtensibleRepresentation, EntityRepresentation
{
    private final MappingRepresentation entity;
    private final ValueRepresentation selfUri;

    @SuppressWarnings( "boxing" )
    public IndexedEntityRepresentation( Node node, String indexPath )
    {
        this( new NodeRepresentation( node ), ValueRepresentation.uri( indexPath ) );
    }

    @SuppressWarnings( "boxing" )
    public IndexedEntityRepresentation( Relationship rel, String indexPath )
    {
        this( new RelationshipRepresentation( rel ), ValueRepresentation.uri( indexPath ) );
    }

    private IndexedEntityRepresentation( MappingRepresentation entity, ValueRepresentation selfUri )
    {
        super( entity.type );
        this.entity = entity;
        this.selfUri = selfUri;
    }

    @Override
    public String getIdentity()
    {
        return ( (ExtensibleRepresentation) entity ).getIdentity();
    }

    public ValueRepresentation selfUri()
    {
        return selfUri;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        entity.serialize( serializer );
        selfUri().putTo( serializer, "indexed" );
    }
}
