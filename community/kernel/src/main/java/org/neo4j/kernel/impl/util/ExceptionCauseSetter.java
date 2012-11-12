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
package org.neo4j.kernel.impl.util;

import java.lang.reflect.Field;

/**
 * Because the JTA spec was built before java exceptions
 * supported "cause", we can't directly set the cause on
 * those exceptions when we want to throw them.
 * 
 * However, since they extend normal java exceptions which
 * today do support "cause", we can set the cause of old exception
 * classes using introspection, which is what this class does.
 */
public class ExceptionCauseSetter {

	public static void setCause(Throwable original, Throwable cause)
	{
		try {
			Field field = Throwable.class.getDeclaredField("cause");
			field.setAccessible(true);
			field.set(original, cause);
		} catch (Exception e) {
			// Failed to set cause. Don't throw an exception from this
			// as we are most likely already recovering from an exception,
			// and being unable to set the cause is most likely a JVM issue
			// that the user can't help anyway.
			// Print an exception from this though, including the cause exception
			// to help debugging.
			
			// TODO: Use proper logging.
			Exception error = new Exception("Unable to set cause of exception (see nested), will print stacktrace of the exception causing all this below.",e);
			error.printStackTrace();
			cause.printStackTrace();
		}
	}
	
}
