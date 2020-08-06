/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Template:
 * <pre>{@code
 *     @Inject
 *     public Neo4jWithSocket server;
 *
 *     @BeforeEach
 *     void setUp( TestInfo testInfo ) throws IOException
 *     {
 *         server.init( testInfo );
 *     }
 *
 *     @AfterEach
 *     void tearDown()
 *     {
 *         server.shutdownDatabase();
 *     }
 * }</pre>
 */
@Inherited
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
@ExtendWith( Neo4jWithSocketSupportExtension.class )
@ResourceLock( value = Neo4jWithSocket.NEO4J_WITH_SOCKET, mode = ResourceAccessMode.READ_WRITE )
public @interface Neo4jWithSocketExtension
{
}
