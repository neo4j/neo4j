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
package org.neo4j.metatest;

import org.junit.Test;

import java.util.concurrent.Callable;

import org.neo4j.test.subprocess.SubProcess;

import static org.junit.Assert.assertEquals;

public class SubProcessTest
{
    private static final String MESSAGE = "message";

    @SuppressWarnings( "serial" )
    private static class TestingProcess extends SubProcess<Callable<String>, String> implements Callable<String>
    {
        private String message;
        private transient volatile boolean started;

        @Override
        protected void startup( String parameter )
        {
            message = parameter;
            started = true;
        }

        @Override
        public String call() throws Exception
        {
            while ( !started )
            // because all calls are asynchronous
            {
                Thread.sleep( 1 );
            }
            return message;
        }
    }

    @Test
    public void canInvokeSubprocessMethod() throws Exception
    {
        Callable<String> subprocess = new TestingProcess().start( MESSAGE );
        try
        {
            assertEquals( MESSAGE, subprocess.call() );
        }
        finally
        {
            SubProcess.stop( subprocess );
        }
    }
}
