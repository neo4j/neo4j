/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rrd;

import org.rrd4j.core.Sample;

import javax.management.MalformedObjectNameException;
import java.io.IOException;
import java.util.Date;

/**
 * Manages sampling the state of the database and storing the samples in a round
 * robin database instance.
 */
public class RrdSampler {
    /**
     * The current sampling object. This is created when calling #start().
     */
    private Sample sample;
    private Sampleable[] samplables;

    /**
     * Keep track of whether to run the update task or not.
     */
    protected RrdSampler(Sample sample, Sampleable... samplables) throws MalformedObjectNameException {
        this.sample = sample;
        this.samplables = samplables;
    }

    /*
     * This method is called each time we want a snapshot of the current system
     * state. Data sources to work with are defined in {@link
     * RrdManager#getRrdDB()}
     */
    public Sample updateSample() {
        try {
            sample.setTime(new Date().getTime());
            for (Sampleable samplable : samplables) {
                sample.setValue(samplable.getName(), samplable.getValue());
            }

            sample.update();
            return sample;
        } catch (IOException e) {
            throw new RuntimeException("IO Error trying to access round robin database path. See nested exception.", e);
        }
    }
}
