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


public final class ScoredRelationshipRepresentation extends
        ScoredEntityRepresentation<RelationshipRepresentation>
{
    public ScoredRelationshipRepresentation(
            RelationshipRepresentation delegate, float score )
    {
        super( delegate, score );
    }

    @Override
    public String getIdentity()
    {
        return getDelegate().getIdentity();
    }

    @Mapping( "type" )
    public ValueRepresentation getType()
    {
        return getDelegate().getType();
    }

    @Mapping( "start" )
    public ValueRepresentation startNodeUri()
    {
        return getDelegate().startNodeUri();
    }

    @Mapping( "end" )
    public ValueRepresentation endNodeUri()
    {
        return getDelegate().endNodeUri();
    }

    @Mapping( "properties" )
    public ValueRepresentation propertiesUri()
    {
        return getDelegate().propertiesUri();
    }

    @Mapping( "property" )
    public ValueRepresentation propertyUriTemplate()
    {
        return getDelegate().propertyUriTemplate();
    }

    @Mapping( "metadata" )
    public MapRepresentation metadata()
    {
        return getDelegate().metadata();
    }
}
