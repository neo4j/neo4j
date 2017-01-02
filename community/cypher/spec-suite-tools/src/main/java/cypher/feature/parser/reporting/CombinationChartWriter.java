/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.parser.reporting;

import java.awt.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import javax.imageio.ImageIO;

import org.jfree.chart.ChartColor;
import org.jfree.chart.renderer.LookupPaintScale;
import org.jfree.data.general.DefaultHeatMapDataset;
import org.jfree.data.general.HeatMapDataset;
import org.jfree.data.general.HeatMapUtilities;
import org.opencypher.tools.io.HtmlTag;

import static org.opencypher.tools.io.HtmlTag.attr;


public class CombinationChartWriter
{
    private final File outDirectory;
    private final String filename;
    private final LookupPaintScale paintScale;

    static final int UPPER_BOUND = 10000;

    public CombinationChartWriter( File outDirectory, String filename )
    {
        this.outDirectory = outDirectory;
        this.filename = filename;
        paintScale = new LookupPaintScale( -1, UPPER_BOUND, Color.WHITE );
        buildPaintScale();
    }

    private void buildPaintScale()
    {
        paintScale.add( -1, ChartColor.BLACK );
        paintScale.add( 0, ChartColor.VERY_DARK_RED );
        paintScale.add( 10, ChartColor.DARK_RED );
        paintScale.add( 25, ChartColor.RED );
        paintScale.add( 50, ChartColor.LIGHT_RED );
        paintScale.add( 75, ChartColor.VERY_LIGHT_RED );
        paintScale.add( 100, ChartColor.VERY_LIGHT_GREEN );
        paintScale.add( 150, ChartColor.LIGHT_GREEN );
        paintScale.add( 200, ChartColor.GREEN );
        paintScale.add( 300, ChartColor.DARK_GREEN );
        paintScale.add( 500, ChartColor.VERY_DARK_GREEN );
    }

    void dumpPNG( List<List<Integer>> data )
    {
        try ( FileOutputStream output = new FileOutputStream( new File( outDirectory, filename + ".png" ) ) )
        {
            ImageIO.write( HeatMapUtilities.createHeatMapImage( createHeatMapDataset( data ), paintScale ),
                    "png", output );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unexpected error during PNG file creation", e );
        }
    }

    public void dumpHTML( List<List<Integer>> data, List<String> tags )
    {
        dumpPNG( data );
        try ( HtmlTag.Html html = HtmlTag.html( new File( outDirectory, filename + ".html" ).toPath() ) )
        {
            try ( HtmlTag body = html.body() )
            {
                buildTable( body, data, tags );
                body.tag( "img", attr( "src", filename + ".png" ) );
            }
        }
    }

    private void buildTable( HtmlTag body, List<List<Integer>> data, List<String> tags )
    {
        try ( HtmlTag table = body.tag( "table", attr( "border", "1" ) ) )
        {
            for ( int i = tags.size() - 1; i > -1; --i )
            {
                try ( HtmlTag row = table.tag( "tr" ) )
                {
                    for ( int j = 0; j < data.get( i ).size(); ++j )
                    {
                        if ( j == i )
                        {
                            final String columns = String.valueOf( j + 1 );
                            row.tag( "td", attr( "colspan", columns ), attr( "align", "right" ) ).text( tags.get( i ) );
                        }
                        else if ( j > i )
                        {
                            row.tag( "td", attr( "align", "center" ), attr( "width", "25px" ),
                                    attr( "height", "25px" ) ).text( data.get( i ).get( j ).toString() );
                        }
                    }
                }
            }
        }
    }

    private HeatMapDataset createHeatMapDataset( List<List<Integer>> data )
    {
        int magnification = 10;
        DefaultHeatMapDataset dataset = new DefaultHeatMapDataset( data.size() * magnification,
                data.size() * magnification, 0, data.size() * magnification, 0, data.size() * magnification );

        for ( int i = data.size() - 1; i > -1; --i )
        {
            for ( int j = 0; j < data.get( i ).size(); ++j )
            {
                double z;
                if ( j < i )
                {
                    z = UPPER_BOUND + 1;
                }
                else if ( j == i )
                {
                    z = -1;
                }
                else
                {
                    z = data.get( i ).get( j );
                }
                // magnify each point to a 10x10 pixel square
                for ( int xi = 0; xi < magnification; ++xi )
                {
                    for ( int yi = 0; yi < magnification; ++yi )
                    {
                        dataset.setZValue( j * magnification + yi, i * magnification + xi, z );
                    }
                }
            }
        }
        return dataset;
    }
}
