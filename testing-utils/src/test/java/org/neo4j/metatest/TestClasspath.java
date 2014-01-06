/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.metatest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;

/**
 * Tests the classpath for the tests
 */
@Documented
public class TestClasspath
{
    @Test
    public void theTestClasspathShouldNotContainTheOriginalArtifacts()
    {
        for ( String lib : System.getProperty("java.class.path").split(":") )
        {
            assertFalse( "test-jar on classpath: " + lib,
                         lib.contains("test-jar") );
        }
    }

    @Test
    public void canLoadSubProcess() throws Exception
    {
        assertNotNull( Class.forName( "org.neo4j.test.subprocess.SubProcess" ) );
    }

    @Test
    public void canAccessJavadocThroughProcessedAnnotation()
    {
        Documented doc = getClass().getAnnotation( Documented.class );
        assertNotNull( "accessing Documented annotation", doc );
        assertEquals( "Tests the classpath for the tests", doc.value().trim() );
    }
}
