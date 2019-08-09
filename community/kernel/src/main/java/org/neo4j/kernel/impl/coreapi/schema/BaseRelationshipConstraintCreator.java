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

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintCreator;

public class BaseRelationshipConstraintCreator extends AbstractConstraintCreator implements ConstraintCreator
{
    final String name;
    final RelationshipType type;

    BaseRelationshipConstraintCreator( InternalSchemaActions actions, String name, RelationshipType type )
    {
        super( actions );
        this.name = name;
        this.type = type;
    }

    @Override
    public ConstraintCreator assertPropertyIsUnique( String propertyKey )
    {
        throw new UnsupportedOperationException( "Uniqueness constraints are not supported on relationships." );
    }

    @Override
    public ConstraintCreator assertPropertyExists( String propertyKey )
    {
        return new RelationshipPropertyExistenceCreator( actions, name, type, propertyKey );
    }

    @Override
    public ConstraintCreator assertPropertyIsNodeKey( String propertyKey )
    {
        throw new UnsupportedOperationException( "Node key constraints are not supported on relationships." );
    }

    @Override
    public ConstraintCreator withName( String name )
    {
        return new BaseRelationshipConstraintCreator( actions, name, type );
    }
}
