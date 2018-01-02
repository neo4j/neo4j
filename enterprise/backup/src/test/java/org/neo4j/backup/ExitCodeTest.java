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
package org.neo4j.backup;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest(BackupTool.class)
public class ExitCodeTest {

    @Test
    public void shouldToolFailureExceptionCauseExitCode() {

        // setup
        PowerMockito.mockStatic(System.class);

        // when
        BackupTool.exitFailure( "tool failed" );

        // then
        PowerMockito.verifyStatic();
        System.exit(1);
    }

    @Test
    public void shouldBackupToolMainCauseExitCode() {

        // setup
        PowerMockito.mockStatic(System.class);

        // when
        BackupTool.main(new String[] {});

        // then
        PowerMockito.verifyStatic();
        System.exit(1);
    }

}
