/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.SchemaDescriptorPredicates;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.SchemaRule;

class IndexLookup implements AutoCloseable
{
    private final List<IndexAccessor> indexAccessors;
    private final Map<IndexRule,IndexReader> readerCache;
    private final SchemaIndexProvider schemaIndexProvider;
    private final PrimitiveIntObjectMap<List<IndexRule>> indexRuleIndex;
    private final IndexSamplingConfig samplingConfig;

    IndexLookup( SchemaStore store, SchemaIndexProvider schemaIndexProvider )
    {
        this.schemaIndexProvider = schemaIndexProvider;
        indexAccessors = new ArrayList<>();
        readerCache = new HashMap<>();
        indexRuleIndex = buildIndexRuleIndex( store );
        samplingConfig = new IndexSamplingConfig( Config.defaults() );
    }

    private PrimitiveIntObjectMap<List<IndexRule>> buildIndexRuleIndex( SchemaStore schemaStore )
    {
        final PrimitiveIntObjectMap<List<IndexRule>> indexRuleIndex = Primitive.intObjectMap();
        for ( SchemaRule schemaRule : schemaStore )
        {
            if ( schemaRule instanceof IndexRule )
            {
                IndexRule rule = (IndexRule) schemaRule;
                int propertyId = rule.schema().getPropertyId(); // assuming 1 property always
                List<IndexRule> ruleList = indexRuleIndex.get( propertyId );
                if ( ruleList == null )
                {
                    ruleList = new LinkedList<>();
                    indexRuleIndex.put( propertyId, ruleList );
                }
                ruleList.add( rule );
            }
        }
        return indexRuleIndex;
    }

    @Override
    public void close() throws IOException
    {
        for ( IndexReader indexReader : readerCache.values() )
        {
            indexReader.close();
        }
        for ( IndexAccessor indexAccessor : indexAccessors )
        {
            indexAccessor.close();
        }
    }

    private IndexRule findIndexRuleWithOneOfLabels( List<IndexRule> indexRules, long[] labelIds )
    {
        for ( long labelId : labelIds )
        {
            for ( IndexRule indexRule : indexRules )
            {
                if ( SchemaDescriptorPredicates.hasLabel( indexRule, (int)labelId ) )
                {
                    return indexRule;
                }
            }
        }
        return null;
    }

    private IndexReader getIndexReader( IndexRule rule ) throws IOException
    {
        IndexReader reader = readerCache.get( rule );
        if ( reader == null )
        {
            IndexAccessor accessor = schemaIndexProvider.getOnlineAccessor(
                    rule.getId(), rule.getIndexDescriptor(), samplingConfig );
            indexAccessors.add( accessor );
            reader = accessor.newReader();
            readerCache.put( rule, reader );
        }
        return reader;
    }

    public Index getAnyIndexOrNull( final long[] labelIds, final int propertyKeyId ) throws IOException
    {
        List<IndexRule> indexRules = indexRuleIndex.get( propertyKeyId );
        if ( indexRules == null )
        {
            return null;
        }
        IndexRule rule = findIndexRuleWithOneOfLabels( indexRules, labelIds );
        if (rule == null)
        {
            return null;
        }
        final IndexReader reader = getIndexReader( rule );

        return ( nodeId, propertyValue ) -> reader.countIndexedNodes( nodeId, propertyValue ) > 0;
    }

    public boolean hasAnyIndexes()
    {
        return !indexRuleIndex.isEmpty();
    }

    interface Index
    {
        boolean contains( long nodeId, Object propertyValue ) throws IOException;
    }
}
