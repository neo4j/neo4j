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
package org.neo4j.graphalgo.path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

/**
 * Data generator that can generate geographic graph data. The generated data is not realistic in terms of how
 * roads or connections would be built, but holds true to geographical aspects in general in terms of location
 * and connection weights.
 */
public class GeoDataGenerator
{
    private final Random random = new Random();
    private final int numberOfNodes;
    private final int numberOfConnections;
    private final int width, height;
    private int maxDistance = 3;
    private double neighborConnectionFactor = 0.6;

    public GeoDataGenerator( int numberOfNodes, double connectionDensity, int width, int height )
    {
        this.numberOfNodes = numberOfNodes;
        this.numberOfConnections = (int) (numberOfNodes * connectionDensity);
        this.width = width;
        this.height = height;
    }

    public GeoDataGenerator withMaxNeighborDistance( int maxDistance, double neighborConnectionFactor )
    {
        this.maxDistance = maxDistance;
        this.neighborConnectionFactor = neighborConnectionFactor;
        return this;
    }

    public void generate( File storeDir ) throws IOException
    {
        BatchInserter inserter = BatchInserters.inserter( storeDir.getAbsoluteFile() );
        Grid grid = new Grid();
        try
        {
            for ( int i = 0; i < numberOfNodes; i++ )
            {
                grid.createNodeAtRandomLocation( random, inserter );
            }

            for ( int i = 0; i < numberOfConnections; i++ )
            {
                grid.createConnection( random, inserter );
            }
        }
        finally
        {
            inserter.shutdown();
        }
    }

    private static class PositionedNode
    {
        private final long node;
        private final double x, y;

        PositionedNode( long node, double x, double y )
        {
            this.node = node;
            this.x = x;
            this.y = y;
        }

        public double distanceTo( PositionedNode other )
        {
            return sqrt( pow( abs( x - other.x ), 2 ) + pow( abs( y - other.y ), 2 ) );
        }
    }

    private static class Cell
    {
        private final List<PositionedNode> nodes = new ArrayList<PositionedNode>();

        void add( PositionedNode node )
        {
            nodes.add( node );
        }

        PositionedNode get( Random random )
        {
            return nodes.get( random.nextInt( nodes.size() ) );
        }
    }

    private class Grid
    {
        private final int cellsX, cellsY;
        private final double sizeX, sizeY;
        private final Cell[][] cells;
        private final Map<String, Object> nodePropertyScratchMap = new HashMap<String, Object>();
        private final Map<String, Object> relationshipPropertyScratchMap = new HashMap<String, Object>();

        Grid()
        {
            this.cellsX = 100;
            this.cellsY = 100;
            this.sizeX = (double)width / cellsX;
            this.sizeY = (double)height / cellsY;
            this.cells = new Cell[cellsX][cellsY];
            for ( int x = 0; x < cellsX; x++ )
            {
                for ( int y = 0; y < cellsY; y++ )
                {
                    cells[x][y] = new Cell();
                }
            }
        }

        void createNodeAtRandomLocation( Random random, BatchInserter inserter )
        {
            double x = random.nextInt( width-1 ) + random.nextFloat();
            double y = random.nextInt( height-1 ) + random.nextFloat();
            nodePropertyScratchMap.put( "x", x );
            nodePropertyScratchMap.put( "y", y );
            long node = inserter.createNode( nodePropertyScratchMap );
            cells[(int)(x/sizeX)][(int)(y/sizeY)].add( new PositionedNode( node, x, y ) );
        }

        void createConnection( Random random, BatchInserter inserter )
        {
            // pick a cell
            int firstCellX = random.nextInt( cellsX );
            int firstCellY = random.nextInt( cellsY );
            int otherCellX = vicinity( random, firstCellX, cellsX );
            int otherCellY = vicinity( random, firstCellY, cellsY );

            // pick another cell at max distance 3
            Cell firstCell = cells[firstCellX][firstCellY];
            Cell otherCell = cells[otherCellX][otherCellY];

            // connect a random node from the first to a random node to the other
            // with a reasonable weight (roughly distance +/- random)
            PositionedNode firstNode = firstCell.get( random );
            PositionedNode otherNode = otherCell.get( random );
            relationshipPropertyScratchMap.put( "weight", factor( random, firstNode.distanceTo( otherNode ), 0.5d ) );
            inserter.createRelationship( firstNode.node, otherNode.node, RelationshipTypes.CONNECTION,
                    relationshipPropertyScratchMap );
        }

        private int vicinity( Random random, int position, int maxPosition )
        {
            int distance = distance( random, maxDistance );
            int result = position + distance * (random.nextBoolean() ? 1 : -1);
            if ( result < 0 )
            {
                result = 0;
            }
            if ( result >= maxPosition )
            {
                result = maxPosition-1;
            }
            return result;
        }

        private int distance( Random random, int max )
        {
            for ( int i = 0; i < max; i++ )
            {
                if ( random.nextFloat() < neighborConnectionFactor )
                {
                    return i;
                }
            }
            return max;
        }

        private double factor( Random random, double value, double maxDivergence )
        {
            double divergence = random.nextDouble()*maxDivergence;
            divergence = random.nextBoolean() ? 1d - divergence : 1d + divergence;
            return value * divergence;
        }
    }

    public static enum RelationshipTypes implements RelationshipType
    {
        CONNECTION;
    }

    public static EstimateEvaluator<Double> estimateEvaluator()
    {
        return new FlatEstimateEvaluator();
    }

    private static class FlatEstimateEvaluator implements EstimateEvaluator<Double>
    {
        private double[] cachedGoal;

        @Override
        public Double getCost( Node node, Node goal )
        {
            if ( cachedGoal == null )
            {
                cachedGoal = new double[] {
                        (Double)goal.getProperty( "x" ),
                        (Double)goal.getProperty( "y" )
                };
            }

            double x = (Double)node.getProperty( "x" ), y = (Double)node.getProperty( "y" );
            double deltaX = abs( x - cachedGoal[0] ), deltaY = abs( y - cachedGoal[1] );
            return sqrt( pow( deltaX, 2 ) + pow( deltaY, 2 ) );
        }
    };
}
