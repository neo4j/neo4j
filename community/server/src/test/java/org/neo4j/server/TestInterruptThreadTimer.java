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

import static org.junit.Assert.fail;

import org.junit.Test;


public class TestInterruptThreadTimer {

	@Test
	public void shouldInterruptIfTimeoutIsReached()
	{
		try {
			InterruptThreadTimer timer = InterruptThreadTimer.createTimer(100, Thread.currentThread());
			timer.startCountdown();
			Thread.sleep(3000);
			fail("Should have been interrupted.");
		} catch(InterruptedException e)
		{
			// ok
		}
	}

	@Test
	public void shouldNotInterruptIfTimeoutIsNotReached()
	{
		try {
			InterruptThreadTimer timer = InterruptThreadTimer.createTimer(1000 * 10, Thread.currentThread());
			timer.startCountdown();
			Thread.sleep(1);
			timer.stopCountdown();
		} catch(InterruptedException e)
		{
			fail("Should not have been interrupted.");
		}
	}
	
}
