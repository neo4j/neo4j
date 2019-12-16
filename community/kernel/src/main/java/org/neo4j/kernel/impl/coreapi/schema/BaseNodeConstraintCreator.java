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

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexConfig;

import static org.neo4j.graphdb.schema.IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap;

public class BaseNodeConstraintCreator extends AbstractConstraintCreator implements ConstraintCreator
{
    protected final Label label;

    public BaseNodeConstraintCreator( InternalSchemaActions actions, String name, Label label, IndexType indexType, IndexConfig indexConfig )
    {
        super( actions, name, indexType, indexConfig );
        this.label = label;

        assertInUnterminatedTransaction();
    }

    @Override
    public ConstraintCreator assertPropertyIsUnique( String propertyKey )
    {
        return new NodePropertyUniqueConstraintCreator( actions, name, label, List.of( propertyKey ), indexType, indexConfig );
    }

    @Override
    public ConstraintCreator assertPropertyExists( String propertyKey )
    {
        return new NodePropertyExistenceConstraintCreator( actions, name, label, List.of( propertyKey ), indexType, indexConfig );
    }

    @Override
    public ConstraintCreator assertPropertyIsNodeKey( String propertyKey )
    {
        return new NodeKeyConstraintCreator( actions, name, label, List.of( propertyKey ), indexType, indexConfig );
    }

    @Override
    public ConstraintCreator withName( String name )
    {
        return new BaseNodeConstraintCreator( actions, name, label, indexType, indexConfig );
    }

    @Override
    public ConstraintCreator withIndexType( IndexType indexType )
    {
        return new BaseNodeConstraintCreator( actions, name, label, indexType, indexConfig );
    }

    @Override
    public ConstraintCreator withIndexConfiguration( Map<IndexSetting,Object> indexConfiguration )
    {
        return new BaseNodeConstraintCreator( actions, name, label, indexType, toIndexConfigFromIndexSettingObjectMap( indexConfiguration ) );
    }
}
