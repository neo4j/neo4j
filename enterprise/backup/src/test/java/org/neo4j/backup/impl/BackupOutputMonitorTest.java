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
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.ParameterisedOutsideWorld;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertTrue;
import static org.neo4j.backup.impl.OnlineBackupCommandCcIT.wrapWithNormalOutput;

public class BackupOutputMonitorTest
{
    Monitors monitors = new Monitors();
    OutsideWorld outsideWorld;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    PrintStream outputStream = wrapWithNormalOutput( System.out, new PrintStream( byteArrayOutputStream ) );

    ByteArrayOutputStream byteArrayErrorStream = new ByteArrayOutputStream();
    PrintStream errorStream = wrapWithNormalOutput( System.err, new PrintStream( byteArrayErrorStream ) );

    @Before
    public void setup()
    {
        outsideWorld = new ParameterisedOutsideWorld( System.console(), outputStream, errorStream, System.in, new DefaultFileSystemAbstraction() );
    }

    @Test
    public void receivingStoreFilesMessageCorrect()
    {
        // given
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld ) );

        // when
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        // then
        assertTrue( byteArrayOutputStream.toString().contains( "Start receiving store files" ) );
    }
}
