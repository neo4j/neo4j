/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.dump.inconsistency;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

/**
 * Entity ids that reported to be inconsistent in consistency report where they where extracted from.
 */
public class ReportInconsistencies implements Inconsistencies
{
    private final MutableLongSet schemaIndexesIds = new LongHashSet();
    private final MutableLongSet relationshipIds = new LongHashSet();
    private final MutableLongSet nodeIds = new LongHashSet();
    private final MutableLongSet propertyIds = new LongHashSet();
    private final MutableLongSet relationshipGroupIds = new LongHashSet();

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
