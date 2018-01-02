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


public final class ScoredNodeRepresentation extends
        ScoredEntityRepresentation<NodeRepresentation>
{
    public ScoredNodeRepresentation( NodeRepresentation delegate, float score )
    {
        super( delegate, score );
    }

    @Mapping( "create_relationship" )
    public ValueRepresentation relationshipCreationUri()
    {
        return getDelegate().relationshipCreationUri();
    }

    @Mapping( "all_relationships" )
    public ValueRepresentation allRelationshipsUri()
    {
        return getDelegate().allRelationshipsUri();
    }

    @Mapping( "incoming_relationships" )
    public ValueRepresentation incomingRelationshipsUri()
    {
        return getDelegate().incomingRelationshipsUri();
    }

    @Mapping( "outgoing_relationships" )
    public ValueRepresentation outgoingRelationshipsUri()
    {
        return getDelegate().outgoingRelationshipsUri();
    }

    @Mapping( "all_typed_relationships" )
    public ValueRepresentation allTypedRelationshipsUriTemplate()
    {
        return getDelegate().allTypedRelationshipsUriTemplate();
    }

    @Mapping( "incoming_typed_relationships" )
    public ValueRepresentation incomingTypedRelationshipsUriTemplate()
    {
        return getDelegate().incomingTypedRelationshipsUriTemplate();
    }

    @Mapping( "outgoing_typed_relationships" )
    public ValueRepresentation outgoingTypedRelationshipsUriTemplate()
    {
        return getDelegate().outgoingTypedRelationshipsUriTemplate();
    }

    @Mapping( "properties" )
    public ValueRepresentation propertiesUri()
    {
        return getDelegate().propertiesUri();
    }

    @Mapping( "labels" )
    public ValueRepresentation labelsUriTemplate()
    {
        return getDelegate().labelsUriTemplate();
    }

    @Mapping( "property" )
    public ValueRepresentation propertyUriTemplate()
    {
        return getDelegate().propertyUriTemplate();
    }

    @Mapping( "traverse" )
    public ValueRepresentation traverseUriTemplate()
    {
        return getDelegate().traverseUriTemplate();
    }

    @Mapping( "paged_traverse" )
    public ValueRepresentation pagedTraverseUriTemplate()
    {
        return getDelegate().pagedTraverseUriTemplate();
    }

    @Mapping( "metadata" )
    public MapRepresentation metadata()
    {
        return getDelegate().metadata();
    }
}
