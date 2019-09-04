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
package org.neo4j.kernel.impl.storemigration.legacy;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.function.Predicates;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

/**
 * A stripped down 3.5.x version of SchemaStorage, used for schema store migration.
 */
public class SchemaStorage35
{
    private final RecordStore<DynamicRecord> schemaStore;

    public SchemaStorage35( RecordStore<DynamicRecord> schemaStore )
    {
        this.schemaStore = schemaStore;
    }

    public Iterable<SchemaRule> getAll()
    {
        return this::loadAllSchemaRules;
    }

    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return loadAllSchemaRules( Predicates.alwaysTrue(), IndexDescriptor.class );
    }

    public IndexDescriptor indexGetForName( String indexName )
    {
        Iterator<IndexDescriptor> itr = indexesGetAll();
        while ( itr.hasNext() )
        {
            IndexDescriptor sid = itr.next();
            if ( sid.getName().equals( indexName ) )
            {
                return sid;
            }
        }
        return null;
    }

    private Iterator<SchemaRule> loadAllSchemaRules()
    {
        return loadAllSchemaRules( Predicates.alwaysTrue(), SchemaRule.class );
    }

    /**
     * Scans the schema store and loads all {@link SchemaRule rules} in it. This method is written with the assumption
     * that there's no id reuse on schema records.
     *
     * @param predicate filter when loading.
     * @param returnType type of {@link SchemaRule} to load.
     * @return {@link Iterator} of the loaded schema rules, lazily loaded when advancing the iterator.
     */
    private <ReturnType extends SchemaRule> Iterator<ReturnType> loadAllSchemaRules( final Predicate<ReturnType> predicate, final Class<ReturnType> returnType )
    {
        return new PrefetchingIterator<>()
        {
            private final long highestId = schemaStore.getHighestPossibleIdInUse();
            private long currentId = 1; /*record 0 contains the block size*/
            private final byte[] scratchData = newRecordBuffer();
            private final DynamicRecord record = schemaStore.newRecord();

            @Override
            protected ReturnType fetchNextOrNull()
            {
                while ( currentId <= highestId )
                {
                    long id = currentId++;
                    schemaStore.getRecord( id, record, RecordLoad.FORCE );
                    if ( record.inUse() && record.isStartRecord() )
                    {
                        // It may be that concurrently to our reading there's a transaction dropping the schema rule
                        // that we're reading and that rule may have spanned multiple dynamic records.
                        try
                        {
                            Collection<DynamicRecord> records;
                            try
                            {
                                records = schemaStore.getRecords( id, RecordLoad.NORMAL, false );
                            }
                            catch ( InvalidRecordException e )
                            {
                                // This may have been due to a concurrent drop of this rule.
                                continue;
                            }

                            SchemaRule schemaRule = SchemaStore35.readSchemaRule( id, records, scratchData );
                            if ( returnType.isInstance( schemaRule ) )
                            {
                                ReturnType returnRule = returnType.cast( schemaRule );
                                if ( predicate.test( returnRule ) )
                                {
                                    return returnRule;
                                }
                            }
                        }
                        catch ( MalformedSchemaRuleException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }
                }
                return null;
            }
        };
    }

    private byte[] newRecordBuffer()
    {
        return new byte[schemaStore.getRecordSize() * 4];
    }
}
