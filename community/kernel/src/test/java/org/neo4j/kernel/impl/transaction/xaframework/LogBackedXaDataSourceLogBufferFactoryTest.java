/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class LogBackedXaDataSourceLogBufferFactoryTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldAllowWritingLogicalLog() throws Exception
    {
        // Given
        ByteBuffer scratch = ByteBuffer.allocate( 1024 );
        LogBackedXaDataSource ds = new LogBackedXaDataSource("irrelephant".getBytes(), "irrelephant")
        {
            private XaLogicalLog logicalLog = new XaLogicalLog( new File( testDir.directory(), "my.log" ), null, null,
                    null, new DefaultFileSystemAbstraction(), new Monitors(), new TestLogging(), null, null,
                    mock( KernelHealth.class ), 100, null );

            @Override
            public XaConnection getXaConnection()
            {
                return null;
            }

            @Override
            public LogBufferFactory createLogBufferFactory()
            {
                return logicalLog.createLogWriter( new Function<Config, File>(){
                    @Override
                    public File apply( Config config )
                    {
                        return new File(testDir.directory(), "my.log");
                    }
                } );
            }
        };

        LogBufferFactory logBufferFactory = ds.createLogBufferFactory();

        // When
        LogBuffer logFile = null;
        try
        {
            logFile = logBufferFactory.createActiveLogFile(
                    new Config(stringMap( store_dir.name(), testDir.absolutePath())), -1 );
            logFile.putLong( 1337l );
            logFile.force();

            // Then the header should be correct
            StoreChannel channel = logFile.getFileChannel();
            channel.position( 0 );
            long[] headerLongs = LogIoUtils.readLogHeader( scratch, channel, true );
            assertThat(headerLongs[0], equalTo(0l));
            assertThat(headerLongs[1], equalTo(-1l));

            // And the data I wrote should immediately follow
            scratch.clear();
            channel.read( scratch );
            scratch.flip();
            assertThat(scratch.getLong(), equalTo(1337l));
        } finally
        {
            if (logFile != null)
                logFile.getFileChannel().close();
        }
    }
}
