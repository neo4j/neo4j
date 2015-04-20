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
package org.neo4j.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;

import org.neo4j.helpers.Provider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

public abstract class ResourceRule<RESOURCE> implements TestRule, Provider<RESOURCE>
{
    public static ResourceRule<File> fileInExistingDirectory( final Provider<? extends FileSystemAbstraction> fs )
    {
        return new ResourceRule<File>()
        {
            @Override
            protected File createResource( Description description )
            {
                File path = path( description );
                fs.instance().mkdir( path.getParentFile() );
                return path;
            }
        };
    }

    public static ResourceRule<File> existingDirectory( final Provider<? extends FileSystemAbstraction> fs )
    {
        return new ResourceRule<File>()
        {
            @Override
            protected File createResource( Description description )
            {
                File path = path( description );
                fs.instance().mkdir( path );
                return path;
            }
        };
    }

    public static ResourceRule<File> testPath()
    {
        return new ResourceRule<File>()
        {
            @Override
            protected File createResource( Description description )
            {
                return path( description );
            }
        };
    }

    public static ResourceRule<PageCache> pageCache( final Provider<? extends FileSystemAbstraction> fs )
    {
        return new ResourceRule<PageCache>()
        {
            final PageCacheRule pageCache = new PageCacheRule();

            @Override
            protected PageCache createResource( Description description )
            {
                return pageCache.getPageCache( fs.instance() );
            }

            @Override
            protected void destroyResource( PageCache done, Throwable failure )
            {
                pageCache.after( failure == null );
            }
        };
    }

    private static File path( Description description )
    {
        return new File( description.getClassName(), description.getMethodName() );
    }

    private RESOURCE resource;

    public final RESOURCE get()
    {
        return instance();
    }

    @Override
    public final RESOURCE instance()
    {
        return resource;
    }

    @Override
    public final Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                resource = createResource( description );
                Throwable failure = null;
                try
                {
                    base.evaluate();
                }
                catch ( Throwable err )
                {
                    failure = err;
                    throw err;
                }
                finally
                {
                    RESOURCE done = resource;
                    resource = null;
                    try
                    {
                        destroyResource( done, failure );
                    }
                    catch ( Throwable err )
                    {
                        if ( failure != null )
                        {
                            failure.addSuppressed( err );
                        }
                        else
                        {
                            throw err;
                        }
                    }
                }
            }
        };
    }

    protected abstract RESOURCE createResource( Description description );

    protected void destroyResource( RESOURCE done, Throwable failure )
    {
    }
}
