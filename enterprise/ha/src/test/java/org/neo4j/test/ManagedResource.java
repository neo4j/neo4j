/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.test.rule.TestDirectory;

public abstract class ManagedResource<R> implements TestRule
{
    private R resource;

    public final R getResource()
    {
        R result = this.resource;
        if ( result == null )
        {
            throw new IllegalStateException( "Resource is not started." );
        }
        return result;
    }

    protected abstract R createResource( TestDirectory dir );

    protected abstract void disposeResource( R resource );

    @Override
    public final Statement apply( final Statement base, Description description )
    {
        final TestDirectory dir = TestDirectory.testDirectory( description.getTestClass() );
        return dir.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                resource = createResource( dir );
                try
                {
                    base.evaluate();
                }
                finally
                {
                    R waste = resource;
                    resource = null;
                    disposeResource( waste );
                }
            }
        }, description );
    }
}
