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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.ResourceRule;

import static org.neo4j.test.ResourceRule.existingDirectory;
import static org.neo4j.test.ResourceRule.fileInExistingDirectory;

public final class Resources implements TestRule
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Life
    {
        InitialLifecycle value();
    }

    public enum InitialLifecycle
    {
        INITIALIZED
                {
                    @Override
                    void initialize( LifeRule life )
                    {
                        life.init();
                    }
                },
        STARTED
                {
                    @Override
                    void initialize( LifeRule life )
                    {
                        life.start();
                    }
                };

        abstract void initialize( LifeRule life );
    }

    public enum TestPath
    {
        EXISTING_DIRECTORY, FILE_IN_EXISTING_DIRECTORY
    }

    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final ResourceRule<PageCache> pageCache = ResourceRule.pageCache( fs );
    private final ResourceRule<File> testPath;
    private final LifeRule life = new LifeRule();

    public Resources()
    {
        this( null );
    }

    public Resources( TestPath path )
    {
        if ( path != null )
        {
            switch ( path )
            {
            case EXISTING_DIRECTORY:
                this.testPath = existingDirectory( fs );
                return;
            case FILE_IN_EXISTING_DIRECTORY:
                this.testPath = fileInExistingDirectory( fs );
                return;
            }
        }
        this.testPath = ResourceRule.testPath();
    }

    @Override
    public Statement apply( Statement base, Description description )
    {
        return fs.apply( testPath.apply( pageCache.apply( lifeStatement( base, description ),
                                                          description ),
                                         description ),
                         description );
    }

    private Statement lifeStatement( Statement base, Description description )
    {
        Life initialLifecycle = description.getAnnotation( Life.class );
        if ( initialLifecycle != null )
        {
            base = initialise( base, initialLifecycle.value() );
        }
        return life.apply( base, description );
    }

    private Statement initialise( final Statement base, final InitialLifecycle initialLifecycle )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                initialLifecycle.initialize( life );
                base.evaluate();
            }
        };
    }

    public FileSystemAbstraction fileSystem()
    {
        return fs.get();
    }

    public PageCache pageCache()
    {
        return pageCache.get();
    }

    public File testPath()
    {
        return testPath.get();
    }

    public void lifeInitialises() throws LifecycleException
    {
        life.init();
    }

    public void lifeStarts() throws LifecycleException
    {
        life.start();
    }

    public void lifeStops() throws LifecycleException
    {
        life.stop();
    }

    public void lifeShutsDown() throws LifecycleException
    {
        life.shutdown();
    }

    public <T> T managed( T service )
    {
        Lifecycle lifecycle = null;
        if ( service instanceof Lifecycle )
        {
            lifecycle = (Lifecycle) service;
        }
        else if ( service instanceof AutoCloseable )
        {
            lifecycle = new Closer( (AutoCloseable) service );
        }
        life.add( lifecycle );
        return service;
    }

    public LogProvider logProvider()
    {
        return NullLogProvider.getInstance();
    }

    private static class Closer extends LifecycleAdapter
    {
        private final AutoCloseable closeable;

        Closer( AutoCloseable closeable )
        {
            this.closeable = closeable;
        }

        @Override
        public void shutdown() throws Exception
        {
            closeable.close();
        }
    }
}
