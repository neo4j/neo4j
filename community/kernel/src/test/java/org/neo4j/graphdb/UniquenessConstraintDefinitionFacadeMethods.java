/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class UniquenessConstraintDefinitionFacadeMethods
{
    private static final FacadeMethod<UniquenessConstraintDefinition> GET_PROPERTY_KEYS =
        new FacadeMethod<UniquenessConstraintDefinition>( "Iterable<String> getPropertyKeys()" )
    {
        @Override
        public void call( UniquenessConstraintDefinition self )
        {
            self.getPropertyKeys();
        }
    };

    static final Iterable<FacadeMethod<UniquenessConstraintDefinition>>
        ALL_UNIQUENESS_CONSTRAINT_DEFINITION_FACADE_METHODS = unmodifiableCollection( asList(
                GET_PROPERTY_KEYS
        ) );
}
