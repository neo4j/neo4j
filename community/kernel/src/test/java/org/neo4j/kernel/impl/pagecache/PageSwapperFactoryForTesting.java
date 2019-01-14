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
package org.neo4j.kernel.impl.pagecache;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;

public class PageSwapperFactoryForTesting
        extends SingleFilePageSwapperFactory
        implements PageSwapperFactory
{
    public static final String TEST_PAGESWAPPER_NAME = "pageSwapperForTesting";

    public static final AtomicInteger createdCounter = new AtomicInteger();
    public static final AtomicInteger configuredCounter = new AtomicInteger();

    public static int countCreatedPageSwapperFactories()
    {
        return createdCounter.get();
    }

    public static int countConfiguredPageSwapperFactories()
    {
        return configuredCounter.get();
    }

    public PageSwapperFactoryForTesting()
    {
        createdCounter.getAndIncrement();
    }

    @Override
    public String implementationName()
    {
        return TEST_PAGESWAPPER_NAME;
    }

    @Override
    public void open( FileSystemAbstraction fs, Configuration configuration )
    {
        super.open( fs, configuration );
        configuredCounter.getAndIncrement();
    }
}
