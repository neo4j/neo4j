/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class ConstraintDefinitionFacadeMethods
{
    private static final FacadeMethod<ConstraintDefinition> GET_LABEL = new FacadeMethod<>( "Label getLabel()", ConstraintDefinition::getLabel );
    private static final FacadeMethod<ConstraintDefinition> GET_RELATIONSHIP_TYPE = new FacadeMethod<>( "RelationshipType getRelationshipType()", ConstraintDefinition::getRelationshipType );
    private static final FacadeMethod<ConstraintDefinition> DROP = new FacadeMethod<>( "void drop()", ConstraintDefinition::drop );
    private static final FacadeMethod<ConstraintDefinition> IS_CONSTRAINT_TYPE = new FacadeMethod<>( "boolean isConstraintType( ConstraintType type )", self -> self.isConstraintType( ConstraintType.UNIQUENESS ) );
    private static final FacadeMethod<ConstraintDefinition> GET_PROPERTY_KEYS = new FacadeMethod<>( "Iterable<String> getPropertyKeys()", ConstraintDefinition::getPropertyKeys );

    static final Iterable<FacadeMethod<ConstraintDefinition>> ALL_CONSTRAINT_DEFINITION_FACADE_METHODS =
            unmodifiableCollection( asList(
                    GET_LABEL,
                    GET_RELATIONSHIP_TYPE,
                    GET_PROPERTY_KEYS,
                    DROP,
                    IS_CONSTRAINT_TYPE
            ) );

    private ConstraintDefinitionFacadeMethods()
    {
    }
}
