package org.neo4j.desktop.ui;

import java.awt.Image;
import java.io.IOException;
import java.util.ArrayList;
import javax.imageio.ImageIO;

import static java.lang.String.format;

public class Graphics
{
    public static final String LOGO_PATTERN = "/neo4j-cherries-%d.png";
    public static final String LOGO_32 = "/neo4j-cherries-32.png";

    public static final String SYSTEM_TRAY_ICON = "/neo4j-systray-16.png";

    static ArrayList<Image> loadIcons()
    {
        ArrayList<Image> icons = new ArrayList<>();
        for ( int i = 16; i <= 256; i *= 2 )
        {
            Image image = loadImage( format( LOGO_PATTERN, i ) );
            if ( null != image )
            {
                icons.add( image );
            }
        }
        return icons;
    }

    static Image loadImage( String resource )
    {
        try
        {
            return ImageIO.read( Components.class.getResource( resource ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
