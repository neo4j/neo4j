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
package org.neo4j.kernel.impl.core;

import org.neo4j.graphdb.RelationshipType;

/**
 * Special sub-class of {@link Token} that implements the public-facing
 * interface {@link RelationshipType}, so that we can pass tokens directly to users
 * without first wrapping them in another object.
 */
public class RelationshipTypeToken extends Token implements RelationshipType
{
    public RelationshipTypeToken( String name, int id )
    {
        super( name, id );
    }

    public static class Factory implements TokenFactory<RelationshipTypeToken>
    {
        @Override
        public RelationshipTypeToken newToken( String name, int id )
        {
            return new RelationshipTypeToken( name, id );
        }
    }
}
