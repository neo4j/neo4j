/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.apache.batik.dom.GenericDOMImplementation;
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

import java.awt.*;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;

public class ChartWriter
{
    private final OutputStream out;

    public ChartWriter( OutputStream out )
    {
        this.out = out;
    }

    public void dumpSVG( Map<String,Integer> data )
    {
        JFreeChart chart = createBarChart( data );

        Document document = GenericDOMImplementation.getDOMImplementation().createDocument( null, "svg", null );
        SVGGraphics2D svgGenerator = new SVGGraphics2D( document );
        chart.draw( svgGenerator, new Rectangle( 1500, 500 ) );

        // Write svg file
        try ( OutputStreamWriter writer = new OutputStreamWriter( out ) )
        {
            svgGenerator.stream( writer, true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unexpected error during SVG file creation", e );
        }
    }

    private JFreeChart createBarChart( Map<String,Integer> data )
    {
        JFreeChart chart = ChartFactory
                .createBarChart( "TCK tag distribution", "Tags", "Occurrences in queries",
                        createCategoryDataset( data ) );

        // styling
        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setBackgroundPaint( Color.lightGray );
        plot.setDomainGridlinePaint( Color.white );
        plot.setDomainGridlinesVisible( true );
        plot.setRangeGridlinePaint( Color.white );
        plot.addRangeMarker( new ValueMarker( 20, Color.BLACK, new BasicStroke( 2 ) ) );

        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setSeriesPaint( 0, ChartColor.DARK_RED );
        renderer.setBarPainter( new StandardBarPainter() );

        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions( CategoryLabelPositions.createUpRotationLabelPositions( Math.PI / 6.0 ) );

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
