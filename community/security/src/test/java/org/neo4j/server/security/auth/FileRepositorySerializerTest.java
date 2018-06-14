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
package org.neo4j.server.security.auth;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assume.assumeTrue;

public class FileRepositorySerializerTest
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();

    @Test
    public void shouldDisallowGlobalReadsOnUnix() throws Exception
    {
        // Given
        assumeTrue( SystemUtils.IS_OS_UNIX );

        File authFile = dir.file( "auth" );
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        UserSerialization serialization = new UserSerialization();

        // When
        serialization.saveRecordsToFile( fs, authFile,
                Collections.singletonList( new User.Builder().withName( "steve_brookreson" ).build() ) );

        // Then
        assertThat( Files.getPosixFilePermissions( authFile.toPath() ),
                containsInAnyOrder(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ));
    }
}
