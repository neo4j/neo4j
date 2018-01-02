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
package org.neo4j.graphdb;

import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class IndexDefinitionFacadeMethods
{
    private static final FacadeMethod<IndexDefinition> GET_LABEL =
        new FacadeMethod<IndexDefinition>( "Label getLabel()" )
        {
            @Override
            public void call( IndexDefinition self )
            {
                self.getLabel();
            }
        };

    private static final FacadeMethod<IndexDefinition> GET_PROPERTY_KEYS =
        new FacadeMethod<IndexDefinition>( "Iterable<String> getPropertyKeys()" )
        {
            @Override
            public void call( IndexDefinition self )
            {
                self.getPropertyKeys();
            }
        };

    private static final FacadeMethod<IndexDefinition> DROP =
        new FacadeMethod<IndexDefinition>( "void drop()" )
        {
            @Override
            public void call( IndexDefinition self )
            {
                self.drop();
            }
        };

    private static final FacadeMethod<IndexDefinition> IS_CONSTRAINT_INDEX =
        new FacadeMethod<IndexDefinition>( "boolean isConstraintIndex()" )
        {
            @Override
            public void call( IndexDefinition self )
            {
                self.isConstraintIndex();
            }
        };

    static final Iterable<FacadeMethod<IndexDefinition>> ALL_INDEX_DEFINITION_FACADE_METHODS =
        unmodifiableCollection( asList(
            GET_LABEL,
            GET_PROPERTY_KEYS,
            DROP,
            IS_CONSTRAINT_INDEX
        ) );
}
