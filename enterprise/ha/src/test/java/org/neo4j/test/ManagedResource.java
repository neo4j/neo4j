/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

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

    protected abstract R createResource( TargetDirectory.TestDirectory dir ) throws Exception;

    protected abstract void disposeResource( R resource );

    @Override
    public final Statement apply( final Statement base, Description description )
    {
        final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( description.getTestClass() );
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
