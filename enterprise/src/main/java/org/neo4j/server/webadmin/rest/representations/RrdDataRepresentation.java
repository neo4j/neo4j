package org.neo4j.server.webadmin.rest.representations;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.server.rest.domain.Representation;
import org.rrd4j.core.FetchData;

public class RrdDataRepresentation implements Representation
{

    private FetchData rrdData;

    public RrdDataRepresentation( FetchData rrdData )
    {
        this.rrdData = rrdData;
    }

    public Object serialize()
    {
        Map<String, Object> data = new HashMap<String, Object>();

        data.put( "start_time", rrdData.getFirstTimestamp() );
        data.put( "end_time", rrdData.getLastTimestamp() );

        data.put( "timestamps", rrdData.getTimestamps() );

        Map<String, Object> datasources = new HashMap<String, Object>();
        for ( int i = 0, l = rrdData.getDsNames().length; i < l; i++ )
        {
            datasources.put( rrdData.getDsNames()[i], rrdData.getValues( i ) );
        }

        data.put( "data", datasources );

        return data;
    }

}

