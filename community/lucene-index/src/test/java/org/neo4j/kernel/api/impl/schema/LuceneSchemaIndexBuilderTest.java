/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.logging.NullLog;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuceneSchemaIndexBuilderTest
{

    public final TestDirectory testDir = TestDirectory.testDirectory();
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = RuleChain
            .outerRule( testDir )
            .around( fileSystemRule )
            .around( pageCacheRule );

    private final IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 0, 0 );

    @Test
    public void readOnlyIndexCreation() throws Exception
    {
        Config config = getReadOnlyConfig();
        try ( SchemaIndex schemaIndex = LuceneSchemaIndexBuilder.create( descriptor, config,
                DirectoryFactory.newDirectoryFactory( pageCacheRule.getPageCache( fileSystemRule ), config,
                        NullLog.getInstance() ))
                .withFileSystem( fileSystemRule.get() )
                .withOperationalMode( OperationalMode.single )
                .withIndexRootFolder( testDir.directory( "a" ) )
                .build() )
        {
            assertTrue( "Builder should construct read only index.", schemaIndex.isReadOnly() );
        }
    }

    @Test
    public void writableIndexCreation() throws Exception
    {
        Config config = getDefaultConfig();
        try ( SchemaIndex schemaIndex = LuceneSchemaIndexBuilder.create( descriptor, config,
                DirectoryFactory.newDirectoryFactory( pageCacheRule.getPageCache( fileSystemRule ), config,
                        NullLog.getInstance() ))
                .withFileSystem( fileSystemRule.get() )
                .withOperationalMode( OperationalMode.single )
                .withIndexRootFolder( testDir.directory( "b" ) )
                .build() )
        {
            assertFalse( "Builder should construct writable index.", schemaIndex.isReadOnly() );
        }
    }

    private Config getDefaultConfig()
    {
        return Config.defaults();
    }

    private Config getReadOnlyConfig()
    {
        return Config.defaults( GraphDatabaseSettings.read_only, Settings.TRUE );
    }
}
