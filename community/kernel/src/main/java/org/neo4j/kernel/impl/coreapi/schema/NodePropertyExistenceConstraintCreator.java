/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexConfig;

import static org.neo4j.graphdb.schema.IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap;

public class NodePropertyExistenceConstraintCreator extends BaseNodeConstraintCreator
{
    private final List<String> propertyKeys;

    NodePropertyExistenceConstraintCreator( InternalSchemaActions actions, String name, Label label, List<String> propertyKeys, IndexType indexType,
            IndexConfig indexConfig )
    {
        super( actions, name, label, indexType, indexConfig );
        this.propertyKeys = propertyKeys;
    }

    @Override
    public ConstraintCreator assertPropertyIsUnique( String propertyKey )
    {
        List<String> keys = List.of( propertyKey );
        if ( propertyKeys.equals( keys ) )
        {
            return new NodeKeyConstraintCreator( actions, name, label, propertyKeys, indexType, indexConfig );
        }
        throw new UnsupportedOperationException(
                "You cannot create a constraint on two different sets of property keys: " + propertyKeys + " vs. " + keys + '.' );
    }

    @Override
    public ConstraintCreator assertPropertyExists( String propertyKey )
    {
        throw new UnsupportedOperationException( "You can only create one property existence constraint at a time." );
    }

    @Override
    public ConstraintCreator assertPropertyIsNodeKey( String propertyKey )
    {
        return assertPropertyIsUnique( propertyKey );
    }

    @Override
    public ConstraintCreator withName( String name )
    {
        return new NodePropertyExistenceConstraintCreator( actions, name, label, propertyKeys, indexType, indexConfig );
    }

    @Override
    public ConstraintCreator withIndexType( IndexType indexType )
    {
        return new NodePropertyExistenceConstraintCreator( actions, name, label, propertyKeys, indexType, indexConfig );
    }

    @Override
    public ConstraintCreator withIndexConfiguration( Map<IndexSetting,Object> indexConfiguration )
    {
        return new NodePropertyExistenceConstraintCreator( actions, name, label, propertyKeys, indexType,
                toIndexConfigFromIndexSettingObjectMap( indexConfiguration ) );
    }

    @Override
    public ConstraintDefinition create()
    {
        if ( indexType != null )
        {
            throw new IllegalArgumentException( "Node property existence constraints cannot be created with an index type. " +
                    "Was given index type " + indexType + '.' );
        }
        if ( indexConfig != null )
        {
            throw new IllegalArgumentException( "Node property existence constraints cannot be created with an index configuration." );
        }
        return actions.createPropertyExistenceConstraint( name, label, propertyKeys.toArray( new String[0] ) );
    }
}
