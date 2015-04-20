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
package org.neo4j.server;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Interrupts a thread after a given timeout, can be cancelled if needed.
 *
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release
 */
@Deprecated
public abstract class InterruptThreadTimer
{
    public enum State
    {
        COUNTING,
        IDLE
    }

    public static class InterruptThreadTask extends TimerTask
	{
		private final Thread threadToInterrupt;
		private boolean wasExecuted = false;

		public InterruptThreadTask(Thread threadToInterrupt)
		{
			this.threadToInterrupt = threadToInterrupt;
		}

		@Override
		public void run()
		{
			wasExecuted = true;
			threadToInterrupt.interrupt();
		}

		public boolean wasExecuted()
		{
			return this.wasExecuted;
		}
	}

	public static InterruptThreadTimer createTimer(long timeoutMillis, Thread threadToInterrupt)
	{
		return new ActualInterruptThreadTimer(timeoutMillis, threadToInterrupt);
	}

	public static InterruptThreadTimer createNoOpTimer()
	{
		return new NoOpInterruptThreadTimer();
	}

	private static class ActualInterruptThreadTimer extends InterruptThreadTimer
	{
		private final Timer timer = new Timer();
		private final InterruptThreadTask task;
		private final long timeout;
        private State state = State.IDLE;

		public ActualInterruptThreadTimer(long timeoutMillis, Thread threadToInterrupt)
		{
			this.task = new InterruptThreadTask(threadToInterrupt);
			this.timeout = timeoutMillis;
		}

		@Override
		public void startCountdown()
		{
            state = State.COUNTING;
			timer.schedule(task, timeout);
		}

		@Override
		public void stopCountdown()
		{
            state = State.IDLE;
			timer.cancel();
		}

        @Override
        public State getState()
        {
            switch ( state )
            {
            case IDLE:
                return State.IDLE;
            case COUNTING:
            default:
                // We don't know if the timeout has triggered at this point,
                // so we need to check that
                if ( wasTriggered() )
                {
                    state = State.IDLE;
                }

                return state;
            }
        }

		@Override
		public boolean wasTriggered()
		{
			return task.wasExecuted();
		}

		@Override
		public long getTimeoutMillis() {
			return timeout;
		}
	}

	private static class NoOpInterruptThreadTimer extends InterruptThreadTimer
	{

        private State state = State.IDLE;

        public NoOpInterruptThreadTimer()
		{
		}

		@Override
		public void startCountdown()
		{
            state = State.COUNTING;
		}

		@Override
		public void stopCountdown()
		{
            state = State.IDLE;
		}

        @Override
        public State getState()
        {
            return state;
        }

		@Override
		public boolean wasTriggered()
		{
			return false;
		}

		@Override
		public long getTimeoutMillis() {
			return 0;
		}
	}

	public abstract void startCountdown();

	public abstract void stopCountdown();

	public abstract boolean wasTriggered();

    public abstract State getState();

	public abstract long getTimeoutMillis();
}
