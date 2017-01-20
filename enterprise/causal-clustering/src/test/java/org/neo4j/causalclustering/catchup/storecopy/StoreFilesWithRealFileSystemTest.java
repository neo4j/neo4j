/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.rules.RuleChain;

import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

public class StoreFilesWithRealFileSystemTest extends StoreFilesTest
{
    @Override
    protected void createRules()
    {
        testDirectory = TestDirectory.testDirectory( StoreFilesTest.class );
        DefaultFileSystemRule defaultFileSystemRule = new DefaultFileSystemRule();
        fileSystemRule = defaultFileSystemRule::get;
        hiddenFileSystemRule = new EphemeralFileSystemRule();
        pageCacheRule = new PageCacheRule( );
        rules = RuleChain.outerRule( defaultFileSystemRule )
                         .around( testDirectory )
                         .around( hiddenFileSystemRule )
                         .around( pageCacheRule );
    }
}
