/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.coreapi.schema;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

abstract class MultiPropertyConstraintDefinition extends PropertyConstraintDefinition
{
    protected final String[] propertyKeys;

    protected MultiPropertyConstraintDefinition( InternalSchemaActions actions, String[] propertyKeys )
    {
        super( actions );
        this.propertyKeys = requireNonEmpty( propertyKeys );
    }

    protected MultiPropertyConstraintDefinition( InternalSchemaActions actions, IndexDefinition indexDefinition )
    {
        super( actions );
        this.propertyKeys = requireNonEmpty( Iterables.asArray( String.class, indexDefinition.getPropertyKeys() ) );
    }

    private static String[] requireNonEmpty( String[] array )
    {
        requireNonNull( array );
        if ( array.length < 1 )
        {
            throw new IllegalArgumentException( "Property constraint must have at least one property" );
        }
        for ( String field : array )
        {
            if ( field == null )
            {
                throw new IllegalArgumentException( "Property constraints cannot have null property names" );
            }
        }
        return array;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        assertInUnterminatedTransaction();
        return asList( propertyKeys );
    }
}
