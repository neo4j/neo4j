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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import org.neo4j.adversaries.fs.AdversarialFileChannel;
import org.neo4j.test.bootclasspathrunner.BootClassPathRunner;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtilTest;

/**
 * Use a special runner that will put the AccessibleFileDispatcher and the DelegateFileDispatcher in the -Xbootclasspath
 *
 * We already have the test-jar from the 'unsafe' module on our classpath. This custom test runner will put that
 * classpath entry on the boot classpath. We refer to that classpath entry through the UnsafeUtilTest class, because
 * we are not allowed to refer to the AccessibleFileDispatcher and the DelegateFileDispatcher classes directly. If we
 * try to load them normally, we will be told that they are not allowed to refer to the sun.nio.ch.FileDispatcher
 * superclass.
 *
 * To implement the boot classpath hack, the BootClassPathRunner will run the test in a sub-process, and tunnel the
 * JUnit communication through RMI.
 */
@BootClassPathRunner.BootEntryOf( UnsafeUtilTest.class )
@RunWith( BootClassPathRunner.class )
public class MuninnPageCacheWithAdversarialFileDispatcherTest extends MuninnPageCacheWithRealFileSystemTest
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
