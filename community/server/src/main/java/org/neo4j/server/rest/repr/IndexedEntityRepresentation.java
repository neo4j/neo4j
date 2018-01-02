/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public final class IndexedEntityRepresentation extends MappingRepresentation implements ExtensibleRepresentation,
        EntityRepresentation
{
    private final MappingRepresentation entity;
    private final ValueRepresentation selfUri;

    @SuppressWarnings( "boxing" )
    public IndexedEntityRepresentation( Node node, String key, String value, IndexRepresentation indexRepresentation )
    {
        this( new NodeRepresentation( node ), node.getId(), key, value, indexRepresentation );
    }

    @SuppressWarnings( "boxing" )
    public IndexedEntityRepresentation( Relationship rel, String key, String value,
            IndexRepresentation indexRepresentation )
    {
        this( new RelationshipRepresentation( rel ), rel.getId(), key, value, indexRepresentation );
    }

    private IndexedEntityRepresentation( MappingRepresentation entity, long entityId, String key, String value,
            IndexRepresentation indexRepresentation )
    {
        super( entity.type );
        this.entity = entity;
        selfUri = ValueRepresentation.uri( indexRepresentation.relativeUriFor( key, value, entityId ) );
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
