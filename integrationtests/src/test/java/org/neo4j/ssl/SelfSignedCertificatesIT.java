/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.ssl;

import org.apache.commons.lang.SystemUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import javax.annotation.Resource;

import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
public class SelfSignedCertificatesIT
{
    @Resource
    public TestDirectory testDirectory;

    @Test
    public void createSelfSignedCertificateWithCorrectPermissions() throws Exception
    {
        assumeTrue( !SystemUtils.IS_OS_WINDOWS );

        PkiUtils certificates = new PkiUtils();
        certificates
                .createSelfSignedCertificate( testDirectory.file( "certificate" ), testDirectory.file( "privateKey" ),
                        "localhost" );

        PosixFileAttributes certificateAttributes =
                Files.getFileAttributeView( testDirectory.file( "certificate" ).toPath(), PosixFileAttributeView.class )
                        .readAttributes();

        assertTrue( certificateAttributes.permissions().contains( PosixFilePermission.OWNER_READ ) );
        assertTrue( certificateAttributes.permissions().contains( PosixFilePermission.OWNER_WRITE ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OWNER_EXECUTE ) );

        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.GROUP_READ ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.GROUP_WRITE ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.GROUP_EXECUTE ) );

        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OTHERS_READ ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OTHERS_WRITE ) );
        assertFalse( certificateAttributes.permissions().contains( PosixFilePermission.OTHERS_EXECUTE ) );

        PosixFileAttributes privateKey =
                Files.getFileAttributeView( testDirectory.file( "privateKey" ).toPath(), PosixFileAttributeView.class )
                        .readAttributes();

        assertTrue( privateKey.permissions().contains( PosixFilePermission.OWNER_READ ) );
        assertTrue( privateKey.permissions().contains( PosixFilePermission.OWNER_WRITE ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.OWNER_EXECUTE ) );

        assertFalse( privateKey.permissions().contains( PosixFilePermission.GROUP_READ ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.GROUP_WRITE ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.GROUP_EXECUTE ) );

        assertFalse( privateKey.permissions().contains( PosixFilePermission.OTHERS_READ ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.OTHERS_WRITE ) );
        assertFalse( privateKey.permissions().contains( PosixFilePermission.OTHERS_EXECUTE ) );
    }
}
