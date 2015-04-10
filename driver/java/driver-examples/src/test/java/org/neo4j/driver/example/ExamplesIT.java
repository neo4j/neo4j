/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.example;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.util.TestSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExamplesIT
{
    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldRunParameterizedExample() throws Throwable
    {
        // Given
        SystemOutCapture capture = new SystemOutCapture();
        capture.captureSystemOut();
        try
        {
            // When
            ParameterizedStatementExample.main();
        }
        finally
        {
            capture.releaseSystemOut();
        }

        // Then
        assertThat( capture.asText(), equalTo("Bob\n") );
    }

    @Test
    public void shouldRunBasicExample() throws Throwable
    {
        // Given
        SystemOutCapture capture = new SystemOutCapture();
        capture.captureSystemOut();
        try
        {
            // When
            BasicDriverExample.main();
        }
        finally
        {
            capture.releaseSystemOut();
        }

        // Then
        assertThat( capture.asText(), equalTo("Bob\n") );
    }

}