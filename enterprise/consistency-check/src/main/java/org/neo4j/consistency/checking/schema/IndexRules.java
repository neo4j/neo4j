/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency.checking.schema;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;

public class IndexRules
{
    @SuppressWarnings("unchecked")
    public static List<IndexRule> loadAllIndexRules( final RecordStore<DynamicRecord> schemaStore )
            throws MalformedSchemaRuleException
    {
        final List<IndexRule> indexRules = new ArrayList<>();
        new RecordStore.Processor<MalformedSchemaRuleException>()
        {
            @Override
            public void processSchema( RecordStore<DynamicRecord> store,
                                       DynamicRecord record ) throws MalformedSchemaRuleException
            {
                if ( record.inUse() && record.isStartRecord() )
                {
                    SchemaRule schemaRule = ((SchemaStore) schemaStore).loadSingleSchemaRule( record.getId() );
                    if ( schemaRule instanceof IndexRule  )
                    {
                        indexRules.add( (IndexRule) schemaRule );
                    }
                }
            }
        }.applyFiltered( schemaStore );
        return indexRules;
    }
}
