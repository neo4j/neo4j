/**
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
package org.neo4j.server.webdriver;

import java.util.Date;

import org.hamcrest.Matcher;

public class Condition<T>
{

    private final T state;
    private final Matcher<T> matcher;
    
    public Condition(Matcher<T> matcher, T state) {
        this.matcher = matcher;
        this.state = state;
    }
    
    public boolean isFulfilled() {
        return matcher.matches( state );
    }
    
    public void waitUntilFulfilled() {
        waitUntilFulfilled( 1000 * 10 );
    }
    
    public void waitUntilFulfilled(long timeout) {
        waitUntilFulfilled(timeout, "Condition was not fulfilled within the time limit ("+timeout+"ms)." );
    }
    
    public void waitUntilFulfilled(long timeout, String errorMessage) {
        timeout = new Date().getTime() + timeout;
        while((new Date().getTime()) < timeout) 
        {
          try
          {
            Thread.sleep( 50 );
          }
          catch ( InterruptedException e )
          {
             throw new RuntimeException(e);
          }
          if ( isFulfilled() ) {
              return;
          }
        }
        
        throw new ConditionTimeoutException( errorMessage );
    }
    
}
