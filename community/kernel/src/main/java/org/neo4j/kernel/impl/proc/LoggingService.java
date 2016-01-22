/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

/**
 * LoggingService provides a logging facility for procedures. It should only be used
 * within a procedure call
 */
public interface LoggingService
{
    boolean isDebugEnabled();

    void error( String message );
    void error( String message, Object... args );
    void error( String message, Throwable throwable );

    void warn( String message );
    void warn( String message, Object... args );
    void warn( String message, Throwable throwable );

    void info( String message );
    void info( String message, Object... args );
    void info( String message, Throwable throwable );

    void debug( String message );
    void debug( String message, Object... args );
    void debug( String message, Throwable throwable );
}
