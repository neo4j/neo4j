/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.generator;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.perftest.enterprise.util.Conversion;

class RelationshipSpec implements RelationshipType
{
    static final Conversion<String, RelationshipSpec> FROM_STRING = new Conversion<String, RelationshipSpec>()
    {
        @Override
        public RelationshipSpec convert( String source )
        {
            String[] parts = source.split( ":" );
            if ( parts.length != 2 )
            {
                throw new IllegalArgumentException( "value must have the format <relationship label>:<count>" );
            }
            return new RelationshipSpec( parts[0], Integer.parseInt( parts[1] ) );
        }
    };
    private final String name;
    final int count;

    public RelationshipSpec( String name, int count )
    {
        this.name = name;
        this.count = count;
    }

    @Override
    public String toString()
    {
        return name() + ":" + count;
    }

    @Override
    public String name()
    {
        return name;
    }
}
