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
package org.neo4j.test.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.extension.ExecutionSharedContext.CONTEXT;
import static org.neo4j.test.extension.ExecutionSharedContext.FAILED_TEST_FILE_KEY;
import static org.neo4j.test.extension.ExecutionSharedContext.SUCCESSFUL_TEST_FILE_KEY;

/**
 * This test class name should not match default test name pattern since it should not be executed by default test launcher
 * Its executed by custom test junit launcher to test extensions lifecycle
 */
@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class DirectoryExtensionLifecycleVerification
{
    @Inject
    private TestDirectory directory;

    @Test
    void executeAndCleanupDirectory()
    {
        File file = directory.createFile( "a" );
        assertTrue( file.exists() );
        CONTEXT.setValue( SUCCESSFUL_TEST_FILE_KEY, file );
    }

    @Test
    void failAndKeepDirectory()
    {
        File file = directory.createFile( "b" );
        CONTEXT.setValue( FAILED_TEST_FILE_KEY, file );
        throw new RuntimeException( "simulate test failure" );
    }
}
