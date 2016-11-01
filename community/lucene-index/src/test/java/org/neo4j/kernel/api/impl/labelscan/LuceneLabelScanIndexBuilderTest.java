/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.labelscan;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuceneLabelScanIndexBuilderTest
{

    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( getClass() );
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void readOnlyIndexCreation() throws Exception
    {
        try ( LabelScanIndex index = LuceneLabelScanIndexBuilder.create()
                .withFileSystem( fileSystemRule.get() )
                .withIndexRootFolder( testDir.graphDbDir() )
                .withConfig( getReadOnlyConfig() )
                .withOperationalMode( OperationalMode.single )
                .build() )
        {
            assertTrue( "Builder should construct read only index.", index.isReadOnly() );
        }
    }

    @Test
    public void writableIndexCreation() throws Exception
    {
        try ( LabelScanIndex index = LuceneLabelScanIndexBuilder.create()
                .withFileSystem( fileSystemRule.get() )
                .withIndexRootFolder( testDir.graphDbDir() )
                .withConfig( getDefaultConfig() )
                .withOperationalMode( OperationalMode.single )
                .build() )
        {
            assertFalse( "Builder should construct writable index.", index.isReadOnly() );
        }
    }

    private Config getDefaultConfig()
    {
        return Config.empty();
    }

    private Config getReadOnlyConfig()
    {
        return getDefaultConfig().with( MapUtil.stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
    }
}
