package org.neo4j.graphalgo.impl.util;

import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphdb.Node;

public class GeoEstimateEvaluator implements EstimateEvaluator<Double>
{
    private static final double EARTH_RADIUS = 6371*1000; // Meters
    
    private Node cachedGoal;
    private double[] cachedGoalCoordinates;
    private final String latitudePropertyKey;
    private final String longitudePropertyKey;
    
    public GeoEstimateEvaluator( String latitudePropertyKey, String longitudePropertyKey )
    {
        this.latitudePropertyKey = latitudePropertyKey;
        this.longitudePropertyKey = longitudePropertyKey;
    }
    
    public Double getCost( Node node, Node goal )
    {
        double[] nodeCoordinates = getCoordinates( node );
        if ( cachedGoal == null || !cachedGoal.equals( goal ) )
        {
            cachedGoalCoordinates = getCoordinates( goal );
            cachedGoal = goal;
        }
        return distance( nodeCoordinates[0], nodeCoordinates[1],
                cachedGoalCoordinates[0], cachedGoalCoordinates[1] );
    }
    
    private double[] getCoordinates( Node node )
    {
        return new double[] {
                ((Number) node.getProperty( latitudePropertyKey )).doubleValue(),
                ((Number) node.getProperty( longitudePropertyKey )).doubleValue()
        };
    }
    
    private double distance( double latitude1, double longitude1,
            double latitude2, double longitude2 )
    {
        latitude1 = Math.toRadians( latitude1 );
        longitude1 = Math.toRadians( longitude1 );
        latitude2 = Math.toRadians( latitude2 );
        longitude2 = Math.toRadians( longitude2 );
        double cLa1 = Math.cos( latitude1 );
        double x_A = EARTH_RADIUS * cLa1 * Math.cos( longitude1 );
        double y_A = EARTH_RADIUS * cLa1 * Math.sin( longitude1 );
        double z_A = EARTH_RADIUS * Math.sin( latitude1 );
        double cLa2 = Math.cos( latitude2 );
        double x_B = EARTH_RADIUS * cLa2 * Math.cos( longitude2 );
        double y_B = EARTH_RADIUS * cLa2 * Math.sin( longitude2 );
        double z_B = EARTH_RADIUS * Math.sin( latitude2 );
        return Math.sqrt( ( x_A - x_B ) * ( x_A - x_B ) + ( y_A - y_B )
                          * ( y_A - y_B ) + ( z_A - z_B ) * ( z_A - z_B ) );
    }
}