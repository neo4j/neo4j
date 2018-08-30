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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

public class IndexCheck implements RecordCheck<IndexEntry, ConsistencyReport.IndexConsistencyReport>
{
    private final long[] entityTokenLongIds;
    private final SchemaDescriptor.PropertySchemaType propertySchemaType;
    private final EntityType entityType;
    private NodeInUseWithCorrectLabelsCheck<IndexEntry,ConsistencyReport.IndexConsistencyReport> nodeChecker;
    private RelationshipInUseWithCorrectRelationshipTypeCheck<IndexEntry,ConsistencyReport.IndexConsistencyReport> relationshipChecker;

    public IndexCheck( StoreIndexDescriptor indexRule )
    {
        SchemaDescriptor schema = indexRule.schema();
        int[] entityTokenIntIds = schema.getEntityTokenIds();
        entityTokenLongIds = new long[entityTokenIntIds.length];
        for ( int i = 0; i < entityTokenIntIds.length; i++ )
        {
            entityTokenLongIds[i] = entityTokenIntIds[i];
        }
        propertySchemaType = schema.propertySchemaType();
        entityType = schema.entityType();
        if ( entityType == EntityType.NODE )
        {
            nodeChecker = new NodeInUseWithCorrectLabelsCheck<>( entityTokenLongIds, propertySchemaType, false );
        }
        if ( entityType == EntityType.RELATIONSHIP )
        {
            relationshipChecker = new RelationshipInUseWithCorrectRelationshipTypeCheck<>( entityTokenLongIds );
        }
    }

    @Override
    public void check( IndexEntry record, CheckerEngine<IndexEntry, ConsistencyReport.IndexConsistencyReport> engine, RecordAccess records )
    {
        long id = record.getId();
        if ( entityType == EntityType.NODE )
        {
            engine.comparativeCheck( records.node( id ), nodeChecker );
        }
        else if ( entityType == EntityType.RELATIONSHIP )
        {
            engine.comparativeCheck( records.relationship( id ), relationshipChecker );
        }
        else
        {
            throw new IllegalStateException( "Don't know how to check index entry of entity type " + entityType );
        }
    }
}
