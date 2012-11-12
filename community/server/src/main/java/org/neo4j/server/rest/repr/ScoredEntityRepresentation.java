/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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


public abstract class ScoredEntityRepresentation<E extends ObjectRepresentation & ExtensibleRepresentation & EntityRepresentation>
        extends ObjectRepresentation implements ExtensibleRepresentation,
        EntityRepresentation
{
    private final E delegate;
    private final float score;

    protected ScoredEntityRepresentation( E scoredObject, float score )
    {
        super( scoredObject.type );
        this.delegate = scoredObject;
        this.score = score;
    }

    protected E getDelegate()
    {
        return delegate;
    }

    @Override
    public String getIdentity()
    {
        return getDelegate().getIdentity();
    }

    @Override
    @Mapping( "self" )
    public ValueRepresentation selfUri()
    {
        return delegate.selfUri();
    }

    @Mapping( "score" )
    public ValueRepresentation score()
    {
        return ValueRepresentation.number( score );
    }

    @Override
    void extraData( MappingSerializer serializer )
    {
        delegate.extraData( serializer );
    }
}
