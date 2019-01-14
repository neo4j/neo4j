/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.ssl;

import org.apache.commons.lang.SystemUtils;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;

import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class SelfSignedCertificatesIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

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
