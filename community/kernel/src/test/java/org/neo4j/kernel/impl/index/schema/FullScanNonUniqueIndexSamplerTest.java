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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import static org.neo4j.helpers.collection.Iterators.array;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.IMMEDIATE;
import static org.neo4j.test.rule.PageCacheRule.config;

public class FullScanNonUniqueIndexSamplerTest
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    protected final RandomRule random = new RandomRule();
    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private final NonUniqueSchemaNumberIndexLayout layout = new NonUniqueSchemaNumberIndexLayout();

    @Test
    public void shouldIncludeAllValuesInTree() throws Exception
    {
        // GIVEN
        List<Number> values = generateNumberValues();
        buildTree( values );

        // WHEN
        IndexSample sample;
        try ( GBPTree<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> gbpTree = newTree( layout ) )
        {
            IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.defaults() );
            FullScanNonUniqueIndexSampler<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> sampler =
                    new FullScanNonUniqueIndexSampler<>( gbpTree, layout, samplingConfig );
            sample = sampler.result();
        }

        // THEN
        assertEquals( values.size(), sample.sampleSize() );
        assertEquals( countUniqueValues( values ), sample.uniqueValues() );
        assertEquals( values.size(), sample.indexSize() );
    }

    static int countUniqueValues( List<Number> values )
    {
        int count = 0;
        Set<Double> seenValues = new HashSet<>();
        for ( Number number : values )
        {
            if ( seenValues.add( number.doubleValue() ) )
            {
                count++;
            }
        }
        return count;
    }

    private List<Number> generateNumberValues()
    {
        List<Number> result = new ArrayList<>();
        for ( IndexEntryUpdate<?> update : NativeSchemaIndexPopulatorTest.someDuplicateIndexEntryUpdates() )
        {
            result.add( (Number) update.values()[0] );
        }
        // TODO: perhaps some more values?
        return result;
    }

    private void buildTree( List<Number> values ) throws IOException
    {
        try ( GBPTree<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> gbpTree = newTree( layout ) )
        {
            try ( Writer<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue> writer = gbpTree.writer() )
            {
                NonUniqueSchemaNumberKey key = layout.newKey();
                NonUniqueSchemaNumberValue value = layout.newValue();
                long nodeId = 0;
                for ( Number number : values )
                {
                    key.from( nodeId, array( number ) );
                    value.from( nodeId, array( number ) );
                    writer.put( key, value );
                    nodeId++;
                }
            }
            gbpTree.checkpoint( IOLimiter.unlimited() );
        }
    }

    private GBPTree<NonUniqueSchemaNumberKey,NonUniqueSchemaNumberValue>
            newTree( NonUniqueSchemaNumberIndexLayout layout ) throws IOException
    {
        return new GBPTree<>(
                pageCacheRule.getPageCache( fs ), directory.file( "tree" ), layout,
                0, NO_MONITOR, NO_HEADER, IMMEDIATE );
    }

    // TODO: shouldIncludeHighestAndLowestPossibleNumberValues
}
