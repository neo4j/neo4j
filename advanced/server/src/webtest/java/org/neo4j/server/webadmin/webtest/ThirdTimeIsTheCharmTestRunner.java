/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.webadmin.webtest;

import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runner.notification.StoppedByUserException;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * We have had a lot of problems with tests failing randomly.
 * <p/>
 * This is the dirtiest, and simplest solution I could come up
 * with. Run the tests. If a test method fails, try again. Try
 * up to three times. If all runs fails, we will fail the test.
 * Otherwise it will mark the test as green.
 * <p/>
 * The name comes from Swedish: "Tredje gången gillt", like the
 * english "Third time is the charm".
 */
public class ThirdTimeIsTheCharmTestRunner extends BlockJUnit4ClassRunner
{
    public ThirdTimeIsTheCharmTestRunner( Class<?> klass ) throws InitializationError
    {
        super( klass );
    }

    @Override
    protected void runChild( FrameworkMethod method, RunNotifier notifier )
    {
        if ( method.getAnnotation( Ignore.class ) != null )
        {
            super.runChild( method, notifier );    // Yes it is ignored. Parent will ignore it properly, not me.
        } else
        {
            MyNotifier myNotifier = new MyNotifier();
            Description description = describeChild( method );
            notifier.fireTestStarted( description );
            try
            {
                do
                {
                    super.runChild( method, myNotifier );
                } while ( myNotifier.failed && ( myNotifier.failures == 1 || myNotifier.failures == 2 ) );

                if ( myNotifier.failures > 2 )
                {
                    notifier.fireTestFailure( myNotifier.failure );
                }

                if ( myNotifier.failures > 0 && !myNotifier.failed )
                {
                    System.out.println( "Test failed at least once. *** Third time is the charm! ***" );
                }

            } finally
            {
                notifier.fireTestFinished( description );
            }
        }
    }

    private class MyNotifier extends RunNotifier
    {
        public int failures = 0;
        private Failure failure;
        private boolean failed;

        @Override
        public void fireTestStarted( Description description ) throws StoppedByUserException
        {
            failed = false;
        }

        @Override
        public void fireTestFailure( Failure failure )
        {
            failed = true;
            this.failure = failure;
            failures++;
            super.fireTestFailure( failure );
        }

    }

}
