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
package org.neo4j.kernel.impl.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class DebugUtilTest
{
    private static final String THE_OTHER_THREAD_NAME = "TheOtherThread";

    @Rule
    public final TestName testName = new TestName();
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( THE_OTHER_THREAD_NAME );

    @Test
    public void shouldFigureOutThatThisIsATest()
    {
        assertThat( DebugUtil.trackTest(), containsString( testName.getMethodName() ) );
        assertThat( DebugUtil.trackTest(), containsString( getClass().getSimpleName() ) );
    }

    @Test
    public void shouldFigureOutThatWeStartedInATest() throws Exception
    {
        new Noise().white();
    }

    private class Noise
    {
        void white()
        {
            assertThat( DebugUtil.trackTest(), containsString( testName.getMethodName() ) );
            assertThat( DebugUtil.trackTest(), containsString( DebugUtilTest.class.getSimpleName() ) );
        }
    }
}
