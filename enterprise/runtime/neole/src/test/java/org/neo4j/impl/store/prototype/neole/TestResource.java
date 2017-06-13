/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.store.prototype.neole;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class TestResource implements TestRule
{
    @Override
    public final Statement apply( Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                before( description );
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    afterFailure( description, failure );
                    throw failure;
                }
                afterSuccess( description );
            }
        };
    }

    protected void before( Description description ) throws Throwable
    {
    }

    protected void afterFailure( Description description, Throwable failure ) throws Throwable
    {
    }

    protected void afterSuccess( Description description ) throws Throwable
    {
    }
}
