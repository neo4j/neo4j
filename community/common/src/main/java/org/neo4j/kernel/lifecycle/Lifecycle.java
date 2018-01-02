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

/**
 * Lifecycle interface for kernel components. Init is called first,
 * followed by start,
 * and then any number of stop-start sequences,
 * and finally stop and shutdown.
 *
 * As a stop-start cycle could be due to change of configuration, please perform anything that depends on config
 * in start().
 *
 * Implementations can throw any exception. Caller must handle this properly.
 *
 * The primary purpose of init in a component is to set up structure: instantiate dependent objects, register handlers/listeners, etc.
 * Only in start should the component actually do anything with this structure. Stop reverses whatever was done in start, and shutdown finally
 * clears any set-up structure, if necessary.
 */
public interface Lifecycle
{
    void init()
        throws Throwable;

    void start()
        throws Throwable;

    void stop()
        throws Throwable;

    void shutdown()
        throws Throwable;

    class Delegate implements Lifecycle
    {
        private final Lifecycle delegate;

        public Delegate( Lifecycle delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void init() throws Throwable
        {
            delegate.init();
        }

        @Override
        public void start() throws Throwable
        {
            delegate.start();
        }

        @Override
        public void stop() throws Throwable
        {
            delegate.stop();
        }

        @Override
        public void shutdown() throws Throwable
        {
            delegate.shutdown();
        }
    }
}
