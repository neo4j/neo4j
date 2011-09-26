/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.statistic;

import java.io.Serializable;

/**
 * storage-class to collect general statistic-data.
 *
 * @author tbaum
 * @since 19.05.11 18:07
 */
public class StatisticData implements Serializable
{
    private static final int MEDIAN_MAX = 3000;
    private int[] requests = new int[MEDIAN_MAX];
    private int count = 0;
    private double sum = 0;
    private double sumSq = 0;
    private double min = 0;
    private double max = 0;

    public StatisticData()
    {
    }

    private StatisticData( int count, double sum, double sumSq, double min,
                           double max, int[] requests )
    {
        this.count = count;
        this.sum = sum;
        this.sumSq = sumSq;
        this.min = min;
        this.max = max;
        System.arraycopy( requests, 0, this.requests, 0, MEDIAN_MAX );
    }


    public double getAvg()
    {
        double avg = 0;
        if ( count > 1 )
        {
            avg = sum / count;
        }
        return avg;
    }

    private double getVar()
    {
        double var = 0;
        if ( count > 2 )
        {
            var = Math.sqrt( ( sumSq - sum * sum / count ) / ( count - 1 ) );
        }
        return var;
    }

    public int getMedian()
    {
        int c = 0;
        int medianPoint = count / 2;
        for ( int i = 0; i < MEDIAN_MAX; i++ )
        {
            c += requests[i];
            if ( c >= medianPoint )
            {
                return i;
            }
        }
        return MEDIAN_MAX;
    }

    @Override
    public String toString()
    {
        return "StatisticData{" +
                "count=" + count +
                ", sum=" + sum +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + getAvg() +
                ", var=" + getVar() +
                ", median=" + getMedian() +
                '}';
    }


    public StatisticData copy()
    {
        return new StatisticData( count, sum, sumSq, min, max, requests );
    }

    public void addValue( double value )
    {
        min = count > 0 && min < value ? min : value;
        max = count > 0 && max > value ? max : value;

        count += 1;
        sum += value;
        sumSq += value * value;

        int v = value > requests.length ? requests.length :
                value < 0 ? 0 :
                        (int) value;

        requests[v]++;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public double getSum()
    {
        return sum;
    }
}


