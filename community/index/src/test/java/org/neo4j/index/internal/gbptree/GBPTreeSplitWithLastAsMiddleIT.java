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
package org.neo4j.index.internal.gbptree;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.test.rule.PageCacheRule.config;

public class GBPTreeSplitWithLastAsMiddleIT
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule );

    @Test
    public void mustStayCorrectWhenInsertingValuesOfIncreasingLength() throws IOException
    {
        Layout<RawBytes,RawBytes> layout = new SimpleByteArrayLayout();
        try ( GBPTree<RawBytes,RawBytes> index = createIndex( layout );
              Writer<RawBytes,RawBytes> writer = index.writer())
        {
            RawBytes emptyValue = layout.newValue();
            emptyValue.bytes = new byte[0];
            for ( int keySize = 1; keySize < index.keyValueSizeCap(); keySize++ )
            {
                RawBytes key = layout.newKey();
                key.bytes = new byte[keySize];
                writer.put( key, emptyValue );
            }
        }
    }

    private GBPTree<RawBytes,RawBytes> createIndex( Layout<RawBytes,RawBytes> layout ) throws IOException
    {
        // some random padding
        PageCache pageCache = pageCacheRule.getPageCache( fs.get(), config().withAccessChecks( true ) );
        return new GBPTreeBuilder<>( pageCache, directory.file( "index" ), layout ).build();
    }
}
