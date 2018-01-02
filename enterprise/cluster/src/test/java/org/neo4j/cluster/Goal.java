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
package org.neo4j.cluster;

import java.util.ArrayList;
import java.util.List;

public class Goal
{
    public static interface SubGoal
    {
        boolean met();
    }
    
    private final List<SubGoal> subGoals = new ArrayList<SubGoal>();
    private final int maxMillisToWait;
    private final int waitAfterAllFulfilled;
    
    public Goal( int maxMillisToWait, int waitAfterAllFulfilled )
    {
        this.maxMillisToWait = maxMillisToWait;
        this.waitAfterAllFulfilled = waitAfterAllFulfilled;
    }
    
    public Goal add( SubGoal subGoal )
    {
        subGoals.add( subGoal );
        return this;
    }
    
    public void await() throws GoalNotMetException
    {
        long endTime = System.currentTimeMillis() + maxMillisToWait;
        while ( !goalsAreMet() && System.currentTimeMillis() < endTime )
            sleep( 100 );
        
        if ( !goalsAreMet() )
            throw new GoalNotMetException( subGoals, "timed out awaiting goals" );
        
        // Wait a while to see if something makes a goal not valid shortly after it has
        // been fulfilled, for example some unexpected state transition.
        sleep( waitAfterAllFulfilled );
        
        if ( !goalsAreMet() )
            throw new GoalNotMetException( subGoals, "goals became unfulfilled after first being fulfilled" );
    }

    private void sleep( int millis )
    {
        try
        {
            Thread.sleep( millis );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
    }

    private boolean goalsAreMet()
    {
        for ( SubGoal subGoal : subGoals )
            if ( !subGoal.met() )
                return false;
        return true;
    }
    
    public static class GoalNotMetException extends Exception
    {
        public GoalNotMetException( List<SubGoal> subGoals, String additionalMessage )
        {
            super( createErrorMessage( subGoals, additionalMessage ) );
        }

        private static String createErrorMessage( List<SubGoal> subGoals, String additionalMessage )
        {
            StringBuilder builder = new StringBuilder( "These goals weren't met (" + additionalMessage + "):" );
            for ( SubGoal subGoal : subGoals )
                if ( subGoal.met() )
                    builder.append( "\n  " + subGoal );
            return builder.toString();
        }
    }
}
