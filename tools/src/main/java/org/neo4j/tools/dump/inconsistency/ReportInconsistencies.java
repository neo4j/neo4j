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
package org.neo4j.tools.dump.inconsistency;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;

/**
 * Entity ids that reported to be inconsistent in consistency report where they where extracted from.
 */
public class ReportInconsistencies implements Inconsistencies
{
    private final PrimitiveLongSet schemaIndexesIds = Primitive.longSet();
    private final PrimitiveLongSet relationshipIds = Primitive.longSet();
    private final PrimitiveLongSet nodeIds = Primitive.longSet();
    private final PrimitiveLongSet propertyIds = Primitive.longSet();
    private final PrimitiveLongSet relationshipGroupIds = Primitive.longSet();

    @Override
    public void relationshipGroup( long id )
    {
        relationshipGroupIds.add( id );
    }

    @Override
    public void schemaIndex( long id )
    {
        schemaIndexesIds.add( id );
    }

    @Override
    public void relationship( long id )
    {
        relationshipIds.add( id );
    }

    @Override
    public void property( long id )
    {
        propertyIds.add( id );
    }

    @Override
    public void node( long id )
    {
        nodeIds.add( id );
    }

    @Override
    public boolean containsNodeId( long id )
    {
        return nodeIds.contains( id );
    }

    @Override
    public boolean containsRelationshipId( long id )
    {
        return relationshipIds.contains( id );
    }

    @Override
    public boolean containsPropertyId( long id )
    {
        return propertyIds.contains( id );
    }

    @Override
    public boolean containsRelationshipGroupId( long id )
    {
        return relationshipGroupIds.contains( id );
    }

    @Override
    public boolean containsSchemaIndexId( long id )
    {
        return schemaIndexesIds.contains( id );
    }
}
