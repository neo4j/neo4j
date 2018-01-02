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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 *  JUnit rule that allows you to manage lifecycle of a set of instances. Register instances
 *  and then use the init/start/stop/shutdown methods.
 */
public class LifeRule implements TestRule
{
    private LifeSupport life = new LifeSupport(  );
    private final boolean autoStart;

    public LifeRule()
    {
        this( false );
    }

    public LifeRule( boolean autoStart )
    {
        this.autoStart = autoStart;
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    if ( autoStart )
                    {
                        start();
                    }
                    base.evaluate();
                    life.shutdown();
                }
                catch ( Throwable failure )
                {
                    try
                    {
                        life.shutdown();
                    }
                    catch ( Throwable suppressed )
                    {
                        failure.addSuppressed( suppressed );
                    }
                    throw failure;
                } finally
                {
                    life = new LifeSupport(  );
                }
            }
        };
    }

    public <T extends Lifecycle> T add( T instance )
    {
        return life.add(instance);
    }


    public void init()
    {
        life.init();
    }

    public void start()
    {
        life.start();
    }

    public void stop()
    {
        life.stop();
    }

    public void shutdown()
    {
        life.shutdown();
    }
}
