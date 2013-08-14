package org.neo4j.desktop.ui;

import java.io.IOException;
import java.io.OutputStream;

public class TeeOutputStream extends OutputStream
{
    private final OutputStream a;
    private final OutputStream b;

    public TeeOutputStream(OutputStream a, OutputStream b)
    {
        this.a = a;
        this.b = b;
    }

    @Override
    public void write( int data ) throws IOException
    {
        a.write( data );
        b.write( data );
    }
}
