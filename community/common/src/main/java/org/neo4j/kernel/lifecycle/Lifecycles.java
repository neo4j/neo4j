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
package org.neo4j.kernel.lifecycle;

public class Lifecycles
{
    private Lifecycles()
    {   // No instances allowed or even necessary
    }

    public static Lifecycle multiple( final Iterable<? extends Lifecycle> lifecycles )
    {
        return new Lifecycle()
        {
            @Override
            public void init() throws Throwable
            {
                for ( Lifecycle lifecycle : lifecycles )
                {
                    lifecycle.init();
                }
            }

            @Override
            public void start() throws Throwable
            {
                for ( Lifecycle lifecycle : lifecycles )
                {
                    lifecycle.start();
                }
            }

            @Override
            public void stop() throws Throwable
            {
                for ( Lifecycle lifecycle : lifecycles )
                {
                    lifecycle.stop();
                }
            }

            @Override
            public void shutdown() throws Throwable
            {
                for ( Lifecycle lifecycle : lifecycles )
                {
                    lifecycle.shutdown();
                }
            }
        };
    }
}
