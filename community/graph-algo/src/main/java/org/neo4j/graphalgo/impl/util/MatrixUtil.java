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
package org.neo4j.graphalgo.impl.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class that hold implementations of vectors and matrices of doubles,
 * all indexed by integers, together with some essential operations on those.
 * @author Patrik Larsson
 */
public class MatrixUtil
{
    /**
     * Vector of doubles
     */
    public static class DoubleVector
    {
        Map<Integer,Double> values = new HashMap<Integer,Double>();

        /**
         * Increment a value in the vector.
         * @param index
         * @param increment
         */
        public void incrementValue( Integer index, double increment )
        {
            Double currentValue = values.get( index );
            if ( currentValue == null )
            {
                currentValue = 0.0;
            }
            currentValue += increment;
            values.put( index, currentValue );
        }

        /**
         * Set a value for a certain index.
         * @param index
         * @param value
         */
        public void set( Integer index, double value )
        {
            values.put( index, value );
        }

        /**
         * Get a value for a certain index.
         * @param index
         * @return The value or null.
         */
        public Double get( Integer index )
        {
            return values.get( index );
        }

        /**
         * Get all indices for which values are stored.
         * @return The indices as a set.
         */
        public Set<Integer> getIndices()
        {
            return values.keySet();
        }

        @Override
        public String toString()
        {
            String res = "";
            int maxIndex = 0;
            for ( Integer i : values.keySet() )
            {
                if ( i > maxIndex )
                {
                    maxIndex = i;
                }
            }
            for ( int i = 0; i <= maxIndex; ++i )
            {
                Double value = values.get( i );
                if ( value == null )
                {
                    value = 0.0;
                }
                res += " " + value;
            }
            return res + "\n";
        }
    }
    /**
     * 2-Dimensional matrix of doubles.
     */
    public static class DoubleMatrix
    {
        Map<Integer,DoubleVector> rows = new HashMap<Integer,DoubleVector>();

        /**
         * Increment a value at a certain position.
         * @param rowIndex
         * @param columnIndex
         * @param increment
         */
        public void incrementValue( Integer rowIndex, Integer columnIndex,
            double increment )
        {
            DoubleVector row = rows.get( rowIndex );
            if ( row == null )
            {
                row = new DoubleVector();
                rows.put( rowIndex, row );
            }
            row.incrementValue( columnIndex, increment );
        }

        /**
         * Set a value at a certain position.
         * @param rowIndex
         * @param columnIndex
         * @param value
         */
        public void set( Integer rowIndex, Integer columnIndex, double value )
        {
            DoubleVector row = rows.get( rowIndex );
            if ( row == null )
            {
                row = new DoubleVector();
                rows.put( rowIndex, row );
            }
            row.set( columnIndex, value );
        }

        /**
         * Get the value at a certain position.
         * @param rowIndex
         * @param columnIndex
         * @return The value or null.
         */
        public Double get( Integer rowIndex, Integer columnIndex )
        {
            DoubleVector row = rows.get( rowIndex );
            if ( row == null )
            {
                return null;
            }
            return row.get( columnIndex );
        }

        /**
         * Gets an entire row as a vector.
         * @param rowIndex
         * @return The row vector or null.
         */
        public DoubleVector getRow( Integer rowIndex )
        {
            return rows.get( rowIndex );
        }

        /**
         * Inserts or replaces an entire row as a vector.
         * @param rowIndex
         * @param row
         */
        public void setRow( Integer rowIndex, DoubleVector row )
        {
            rows.put( rowIndex, row );
        }

        @Override
        public String toString()
        {
            String res = "";
            for ( Integer i : rows.keySet() )
            {
                res += rows.get( i ).toString();
            }
            return res;
        }

        /**
         * @return The number of rows in the matrix.
         */
        public int size()
        {
            return rows.keySet().size();
        }
    }

    /**
     * Destructive (in-place) LU-decomposition
     * @param matrix
     *            input
     */
    // TODO: extend to LUP?
    public static void LUDecomposition( DoubleMatrix matrix )
    {
        int matrixSize = matrix.size();
        for ( int i = 0; i < matrixSize - 1; ++i )
        {
            double pivot = matrix.get( i, i );
            DoubleVector row = matrix.getRow( i );
            for ( int r = i + 1; r < matrixSize; ++r )
            {
                Double rowStartValue = matrix.get( r, i );
                if ( rowStartValue == null || rowStartValue == 0.0 )
                {
                    continue;
                }
                double factor = rowStartValue / pivot;
                matrix.set( r, i, factor );
                for ( Integer c : row.values.keySet() )
                {
                    if ( c <= i )
                    {
                        continue;
                    }
                    matrix.incrementValue( r, c, -row.get( c ) * factor );
                }
            }
        }
    }

    /**
     * Solves the linear equation system ax = b.
     * @param a
     *            Input matrix. Will be altered in-place.
     * @param b
     *            Input vector. Will be altered in-place.
     * @return the vector x solving the equations.
     */
    public static DoubleVector LinearSolve( DoubleMatrix a, DoubleVector b )
    {
        LUDecomposition( a );
        // first solve Ly = b ...
        for ( int r = 0; r < a.size(); ++r )
        {
            DoubleVector row = a.getRow( r );
            for ( Integer c : row.values.keySet() )
            {
                if ( c >= r )
                {
                    continue;
                }
                b.incrementValue( r, -row.get( c ) * b.get( c ) );
            }
        }
        // ... then Ux = y
        for ( int r = a.size() - 1; r >= 0; --r )
        {
            DoubleVector row = a.getRow( r );
            for ( Integer c : row.values.keySet() )
            {
                if ( c <= r )
                {
                    continue;
                }
                b.incrementValue( r, -row.get( c ) * b.get( c ) );
            }
            b.set( r, b.get( r ) / row.get( r ) );
        }
        return b;
    }

    /**
     * Multiplies a matrix and a vector.
     * @param matrix
     * @param vector
     * @return The result as a new vector.
     */
    public static DoubleVector multiply( DoubleMatrix matrix,
        DoubleVector vector )
    {
        DoubleVector result = new DoubleVector();
        for ( int rowIndex = 0; rowIndex < matrix.size(); ++rowIndex )
        {
            DoubleVector row = matrix.getRow( rowIndex );
            for ( Integer index : row.getIndices() )
            {
                result.incrementValue( rowIndex, row.get( index )
                    * vector.get( index ) );
            }
        }
        return result;
    }

    /**
     * In-place normalization of a vector.
     * @param vector
     * @return The initial euclidean length of the vector.
     */
    public static double normalize( DoubleVector vector )
    {
        double len = 0;
        for ( Integer index : vector.getIndices() )
        {
            Double d = vector.get( index );
            len += d * d;
        }
        len = Math.sqrt( len );
        for ( Integer index : vector.getIndices() )
        {
            vector.set( index, vector.get( index ) / len );
        }
        return len;
    }
}
