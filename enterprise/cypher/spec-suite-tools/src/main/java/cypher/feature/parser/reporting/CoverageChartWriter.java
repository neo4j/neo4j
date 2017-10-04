/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package cypher.feature.parser.reporting;

import org.apache.batik.svggen.SVGGraphics2D;
import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.w3c.dom.Document;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Rectangle;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import javax.imageio.ImageIO;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class CoverageChartWriter
{
    private final File outDirectory;
    private final String filename;

    private static final int HORIZONTAL_LINE_VALUE = 100;

    public CoverageChartWriter( File outDirectory, String filename )
    {
        this.outDirectory = outDirectory;
        this.filename = filename;
    }

    public void dumpSVG( Map<String,Integer> data )
    {
        try
        {
            SVGGraphics2D svgGenerator = new SVGGraphics2D( getDocument() );
            createBarChart( data ).draw( svgGenerator, new Rectangle( 1500, 500 ) );
            try ( OutputStreamWriter writer = new OutputStreamWriter(
                    new FileOutputStream( new File( outDirectory, filename + ".svg" ) ) ) )
            {
                svgGenerator.stream( writer, true );
            }
        }
        catch ( Exception e )
        {
            System.err.println( "Failed to write test report chart to SVG: " + e.getMessage() );
            e.printStackTrace( System.err );
        }
    }

    private Document getDocument()
    {
        try
        {
            return DocumentBuilderFactory.newInstance().newDocumentBuilder().getDOMImplementation()
                    .createDocument( null, "svg", null );
        }
        catch ( ParserConfigurationException e )
        {
            throw new RuntimeException( "Unexpected error during DOM document creation", e );
        }
    }

    public void dumpPNG( Map<String,Integer> data )
    {
        try
        {
            try ( FileOutputStream output = new FileOutputStream( new File( outDirectory, filename + ".png" ) ) )
            {
                ImageIO.write( createBarChart( data ).createBufferedImage( 1500, 500 ), "png", output );
            }
        }
        catch ( Exception e )
        {
            System.err.println( "Failed to write test report chart to PNG: " + e.getMessage() );
            e.printStackTrace( System.err );
        }
    }

    private JFreeChart cached;

    private JFreeChart createBarChart( Map<String,Integer> data )
    {
        if ( cached != null )
        {
            return cached;
        }
        JFreeChart chart = ChartFactory
                .createBarChart( "Spec suite tag distribution", "Tags", "Occurrences in queries",
                        createCategoryDataset( data ) );

        // styling
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setBackgroundPaint( Color.lightGray );
        plot.setDomainGridlinePaint( Color.white );
        plot.setDomainGridlinesVisible( true );
        plot.setRangeGridlinePaint( Color.white );
        plot.addRangeMarker( new ValueMarker( HORIZONTAL_LINE_VALUE, Color.BLACK, new BasicStroke( 2 ) ) );

        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setSeriesPaint( 0, ChartColor.DARK_RED );
        renderer.setBarPainter( new StandardBarPainter() );

        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions( CategoryLabelPositions.createUpRotationLabelPositions( Math.PI / 6.0 ) );

        cached = chart;
        return chart;
    }

    private CategoryDataset createCategoryDataset( Map<String,Integer> data )
    {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for ( Map.Entry<String,Integer> entry : data.entrySet() )
        {
            dataset.addValue( entry.getValue(), "tag", entry.getKey() );
        }
        return dataset;
    }

}
