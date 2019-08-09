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
package org.neo4j.test.extension;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DbmsExtension
class DbmsExtensionInjectionTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseManagementService dbms;
    @Inject
    private GraphDatabaseService db;
    @Inject
    private GraphDatabaseAPI dbApi;

    @Test
    void shouldInject()
    {
        assertNotNull( fs );
        assertNotNull( testDirectory );
        assertNotNull( dbms );
        assertNotNull( db );
        assertNotNull( dbApi );

        assertEquals( testDirectory.getFileSystem(), fs );
        assertTrue( fs instanceof DefaultFileSystemAbstraction );

        assertSame( db, dbApi );
    }

    @Nested
    class NestedTest
    {
        @Test
        void injectedFieldsShouldBeAvailableForNestedTests()
        {
            shouldInject();
        }
    }
}
