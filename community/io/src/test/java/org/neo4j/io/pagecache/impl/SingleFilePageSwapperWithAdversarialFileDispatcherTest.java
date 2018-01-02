/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import org.neo4j.adversaries.fs.AdversarialFileChannel;
import org.neo4j.test.bootclasspathrunner.BootClassPathRunner;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtilTest;

@BootClassPathRunner.BootEntryOf( UnsafeUtilTest.class )
@RunWith( BootClassPathRunner.class )
public class SingleFilePageSwapperWithAdversarialFileDispatcherTest extends SingleFilePageSwapperWithRealFileSystemTest
{
    @BeforeClass
    public static void enableAdversarialFileDispatcher()
    {
        AdversarialFileChannel.useAdversarialFileDispatcherHack = true;
    }

    @AfterClass
    public static void disableAdversarialFileDispatcher()
    {
        AdversarialFileChannel.useAdversarialFileDispatcherHack = false;
    }
}
