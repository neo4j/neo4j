/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.beans.PropertyVetoException;
import java.beans.VetoableChangeListener;
import java.beans.VetoableChangeSupport;
import java.util.Date;
import org.neo4j.helpers.Specification;
import org.neo4j.helpers.Specifications;

/**
 * Implementation of the CircuitBreaker pattern
 */
public class CircuitBreaker
{
   public enum Status
   {
      off,
      on
   }

   private int threshold;
   private long timeout;
   private Specification<Throwable> allowedThrowables;

   private int countDown;
   private long trippedOn = -1;
   private long enableOn = -1;

   private Status status = Status.on;

   private Throwable lastThrowable;

   PropertyChangeSupport pcs = new PropertyChangeSupport(this);
   VetoableChangeSupport vcs = new VetoableChangeSupport(this);

   public CircuitBreaker( int threshold, long timeout, Specification<Throwable> allowedThrowables )
   {
      this.threshold = threshold;
      this.countDown = threshold;
      this.timeout = timeout;
      this.allowedThrowables = allowedThrowables;
   }

   public CircuitBreaker(int threshold, long timeout)
   {
      this(threshold, timeout, Specifications.not( Specifications.<Throwable>TRUE())); // Trip on all exceptions as default
   }

   public CircuitBreaker()
   {
      this(1, 1000*60*5); // 5 minute timeout as default
   }

   public synchronized void trip()
   {
      if (status == Status.on)
      {
         if (countDown != 0)
         {
            // If this was invoked manually, then set countDown to zero automatically
            int oldCountDown = countDown;
            countDown = 0;
            pcs.firePropertyChange( "serviceLevel", (oldCountDown)/((double)threshold), countDown/((double)threshold) );
         }

         status = Status.off;
         pcs.firePropertyChange( "status", Status.on, Status.off );

         trippedOn = System.currentTimeMillis();
         enableOn = trippedOn+timeout;
      }
   }

   public synchronized void turnOn() throws PropertyVetoException
   {
      if (status == Status.off)
      {
         try
         {
            vcs.fireVetoableChange( "status", Status.off, Status.on );
            status = Status.on;
            countDown = threshold;
            trippedOn = -1;
            enableOn = -1;
            lastThrowable = null;

            pcs.firePropertyChange( "status", Status.off, Status.on );
         } catch (PropertyVetoException e)
         {
            // Reset timeout
            enableOn = System.currentTimeMillis()+timeout;

            if (e.getCause() != null)
               lastThrowable = e.getCause();
            else
               lastThrowable = e;
            throw e;
         }
      }
   }

   public int getThreshold()
   {
      return threshold;
   }

   public synchronized Throwable getLastThrowable()
   {
      return lastThrowable;
   }

   public synchronized double getServiceLevel()
   {
      return countDown/((double)threshold);
   }

   public synchronized Status getStatus()
   {
      if (status == Status.off)
      {
         if (System.currentTimeMillis() > enableOn)
         {
            try
            {
               turnOn();
            } catch (PropertyVetoException e)
            {
               if (e.getCause() != null)
                  lastThrowable = e.getCause();
               else
                  lastThrowable = e;
            }
         }
      }

      return status;
   }

   public Date getTrippedOn()
   {
      return trippedOn == -1 ? null : new Date(trippedOn);
   }

   public Date getEnableOn()
   {
      return enableOn == -1 ? null : new Date(enableOn);
   }

   public boolean isOn()
   {
      return getStatus().equals( Status.on );
   }

   public synchronized void throwable(Throwable throwable)
   {
      if ( status == Status.on)
      {
         if (allowedThrowables.satisfiedBy( throwable ))
         {
            // Allowed throwable, so counts as success
            success();
         } else
         {
            countDown--;

            lastThrowable = throwable;

            pcs.firePropertyChange( "serviceLevel", (countDown+1)/((double)threshold), countDown/((double)threshold) );

            if (countDown == 0)
            {
               trip();
            }
         }
      }
   }

   public synchronized void success()
   {
      if (status == Status.on && countDown < threshold)
      {
         countDown++;

         pcs.firePropertyChange( "serviceLevel", (countDown-1)/((double)threshold), countDown/((double)threshold) );
      }
   }

   public void addVetoableChangeListener( VetoableChangeListener vcl)
   {
      vcs.addVetoableChangeListener( vcl );
   }

   public void removeVetoableChangeListener( VetoableChangeListener vcl)
   {
      vcs.removeVetoableChangeListener( vcl );
   }

   public void addPropertyChangeListener( PropertyChangeListener pcl)
   {
      pcs.addPropertyChangeListener( pcl );
   }

   public void removePropertyChangeListener( PropertyChangeListener pcl)
   {
      pcs.removePropertyChangeListener( pcl );
   }
}
