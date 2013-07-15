package org.neo4j.desktop.ui;

import java.awt.Image;
import java.io.IOException;

import javax.imageio.ImageIO;

public class UIHelper
{
    public static Image loadImage( String resource )
    {
        try
        {
            return ImageIO.read( UIHelper.class.getResource( resource ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private UIHelper()
    {
    }
}
