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
package org.neo4j.commandline.admin;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.system.SystemExitRule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RealOutsideWorldTest
{
    @Rule
    public SystemExitRule systemExitRule = SystemExitRule.none();

    @Test
    public void closeFileSystemOnClose() throws Exception
    {
        RealOutsideWorld outsideWorld = new RealOutsideWorld();
        FileSystemAbstraction fileSystemMock = mock( FileSystemAbstraction.class );
        outsideWorld.fileSystemAbstraction = fileSystemMock;

        outsideWorld.close();

        verify( fileSystemMock ).close();
    }

    @Test
    public void closeFilesystemOnExit() throws IOException
    {
        RealOutsideWorld outsideWorld = new RealOutsideWorld();
        FileSystemAbstraction fileSystemMock = mock( FileSystemAbstraction.class );
        outsideWorld.fileSystemAbstraction = fileSystemMock;

        systemExitRule.expectExit( 0 );

        outsideWorld.exit( 0 );

        verify( fileSystemMock ).close();
    }
}
