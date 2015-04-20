/*
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
package org.neo4j.test.server;

import static org.neo4j.test.Mute.muteAll;

import java.util.concurrent.Callable;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.neo4j.test.Mute;
import org.neo4j.test.TargetDirectory;

public class ExclusiveServerTestBase
{
    public TargetDirectory folder = TargetDirectory.forTest( getClass() );

    @Rule
    public Mute mute = muteAll();
    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static final void ensureServerNotRunning() throws Exception
    {
        muteAll().call( new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                ServerHolder.ensureNotRunning();
                return null;
            }
        } );
    }
}
