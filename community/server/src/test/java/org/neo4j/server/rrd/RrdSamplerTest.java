/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rrd;

import org.junit.Test;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.Sample;

import java.io.IOException;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RrdSamplerTest
{
    @Test
    public void canSampleADatabase() throws IOException
    {
        Sampleable testSamplable = new TestSamplable( "myTest", 15 );

        RrdDb rrd = mock( RrdDb.class );
        final Sample sample = mock( Sample.class );
        when( rrd.createSample( anyLong() ) ).thenReturn( sample );

        RrdSampler sampler = new RrdSamplerImpl( rrd, testSamplable );
        sampler.updateSample();

        verify( sample ).setValue( "myTest", 15 );
    }

    private class TestSamplable implements Sampleable
    {
        private String name;
        private double value;

        private TestSamplable( String name, double value )
        {
            this.name = name;
            this.value = value;
        }

        public String getName()
        {
            return name;
        }

        public double getValue()
        {
            return value;
        }

        public DsType getType()
        {
            return DsType.GAUGE;
        }
    }
}
