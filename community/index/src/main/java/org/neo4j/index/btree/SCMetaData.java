package org.neo4j.index.btree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.neo4j.graphdb.Direction;
import org.neo4j.index.SCIndexDescription;

/**
 * META DATA FORMAT
 *
 * Firstlabel
 * REALTIONSHIPTYPE DIRECTION [propertyKey]
 * Secondlabel [propertyKey]
 * pagesize
 * rootid
 *
 * example
 * Person
 * CREATED OUTGOING
 * Comment date
 * 8192
 * 0
 */
public class SCMetaData
{
    private static final String separator = " ";
    public final  SCIndexDescription description;
    public final int pageSize;
    public final long rootId;
    public final long lastId;

    public SCMetaData( SCIndexDescription description, int pageSize, long rootId, long lastId )
    {
        this.description = description;
        this.pageSize = pageSize;
        this.rootId = rootId;
        this.lastId = lastId;
    }

    public static SCMetaData parseMeta( File metaFile ) throws IOException
    {
        String firstLabel;
        String[] relTypeLine;
        String[] secondLabelLine;
        String pageSizeLine;
        String rootIdLine;
        String lastIdLine;
        try ( BufferedReader br = new BufferedReader( new FileReader( metaFile ) ) ) {
            firstLabel = br.readLine();
            relTypeLine = br.readLine().split( separator );
            secondLabelLine = br.readLine().split( separator );
            pageSizeLine = br.readLine();
            rootIdLine = br.readLine();
            lastIdLine = br.readLine();

            br.close();
        }
        Direction dir;
        switch ( relTypeLine[1] )
        {
        case "OUTGOING":
            dir = Direction.OUTGOING;
            break;
        case "INCOMING":
            dir = Direction.INCOMING;
            break;
        case "BOTH":
            dir = Direction.BOTH;
            break;
        default:
            throw new MetaDataException( "Direction was " + relTypeLine[1] + ". Need to be OUTGOING or INCOMING." );
        }

        // Get property key
        String relationshipPropertyKey = null;
        String nodePropertyKey = null;
        if ( relTypeLine.length == 3 && secondLabelLine.length == 1 )
        {
            // Property on relationship
            relationshipPropertyKey = relTypeLine[2];
        }
        else if ( relTypeLine.length == 2 && secondLabelLine.length == 2 )
        {
            // Property on second node
            nodePropertyKey = secondLabelLine[1];
        }
        else
        {
            // No property. Bad
            throw new MetaDataException( "Property needs to be defined in one and only one place" );
        }

        SCIndexDescription description = new SCIndexDescription( firstLabel, secondLabelLine[0], relTypeLine[0], dir,
                relationshipPropertyKey, nodePropertyKey );

        int pageSize = Integer.parseInt( pageSizeLine );

        long rootId = Long.parseLong( rootIdLine );

        long lastId = Long.parseLong( lastIdLine );

        return new SCMetaData( description, pageSize, rootId, lastId );
    }

    public static void writeMetaData( File metaFile, SCIndexDescription description,
            int pageSize, long rootId, long lastId ) throws FileNotFoundException
    {
        String firstLabel = description.firstLabel;
        String direction;
        switch ( description.direction )
        {
        case OUTGOING:
            direction = "OUTGOING";
            break;
        case INCOMING:
            direction = "INCOMING";
            break;
        case BOTH:
            direction = "BOTH";
            break;
        default:
            throw new RuntimeException( "Invalid direction: " + description.direction );
        }
        String relTypeLine = description.relationshipType + separator + direction + separator +
                             ( description.relationshipPropertyKey != null ? description.relationshipPropertyKey : "" );

        String secondLabelLine = description.secondLabel + separator +
                                 ( description.nodePropertyKey != null ? description.nodePropertyKey : "" );
        String pageSizeLine = Integer.toString( pageSize );
        String rootIdLine = Long.toString( rootId );
        String lastIdLine = Long.toString( lastId );

        PrintWriter out = new PrintWriter( metaFile );
        out.println( firstLabel );
        out.println( relTypeLine );
        out.println( secondLabelLine );
        out.println( pageSizeLine );
        out.println( rootIdLine );
        out.println( lastIdLine );
        out.close();
    }

    public static class MetaDataException extends IllegalArgumentException
    {
        public MetaDataException( String msg )
        {
            super( msg );
        }
    }
}
