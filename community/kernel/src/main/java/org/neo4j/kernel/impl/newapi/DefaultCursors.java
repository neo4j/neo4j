/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.newapi;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.internal.kernel.api.AutoCloseablePlus;
import org.neo4j.internal.kernel.api.CursorFactory;

import static java.lang.String.format;
import static org.neo4j.util.FeatureToggles.flag;

public class DefaultCursors implements CursorFactory
{
    private DefaultNodeCursor nodeCursor;
    private DefaultRelationshipScanCursor relationshipScanCursor;
    private DefaultRelationshipTraversalCursor relationshipTraversalCursor;
    private DefaultPropertyCursor propertyCursor;
    private DefaultRelationshipGroupCursor relationshipGroupCursor;
    private DefaultNodeValueIndexCursor nodeValueIndexCursor;
    private DefaultNodeLabelIndexCursor nodeLabelIndexCursor;
    private DefaultNodeExplicitIndexCursor nodeExplicitIndexCursor;
    private DefaultRelationshipExplicitIndexCursor relationshipExplicitIndexCursor;

    private static final boolean DEBUG_CLOSING = flag( DefaultCursors.class, "trackCursors", false );
    private List<CloseableStacktrace> closeables = new ArrayList<>();

    @Override
    public DefaultNodeCursor allocateNodeCursor()
    {
        if ( nodeCursor == null )
        {
            return trace( new DefaultNodeCursor( this ) );
        }

        try
        {
            return nodeCursor;
        }
        finally
        {
            nodeCursor = null;
        }
    }

    public void accept( DefaultNodeCursor cursor )
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
        }
        nodeCursor = cursor;
    }

    @Override
    public DefaultRelationshipScanCursor allocateRelationshipScanCursor()
    {
        if ( relationshipScanCursor == null )
        {
            return trace( new DefaultRelationshipScanCursor( this ) );
        }

        try
        {
            return relationshipScanCursor;
        }
        finally
        {
            relationshipScanCursor = null;
        }
    }

    public void accept( DefaultRelationshipScanCursor cursor )
    {
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
        }
        relationshipScanCursor = cursor;
    }

    @Override
    public DefaultRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        if ( relationshipTraversalCursor == null )
        {
            return trace( new DefaultRelationshipTraversalCursor( new DefaultRelationshipGroupCursor( null ), this ) );
        }

        try
        {
            return relationshipTraversalCursor;
        }
        finally
        {
            relationshipTraversalCursor = null;
        }
    }

    public void accept( DefaultRelationshipTraversalCursor cursor )
    {
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
        }
        relationshipTraversalCursor = cursor;
    }

    @Override
    public DefaultPropertyCursor allocatePropertyCursor()
    {
        if ( propertyCursor == null )
        {
            return trace( new DefaultPropertyCursor( this ) );
        }

        try
        {
            return propertyCursor;
        }
        finally
        {
            propertyCursor = null;
        }
    }

    public void accept( DefaultPropertyCursor cursor )
    {
        if ( propertyCursor != null )
        {
            propertyCursor.release();
        }
        propertyCursor = cursor;
    }

    @Override
    public DefaultRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        if ( relationshipGroupCursor == null )
        {
            return trace( new DefaultRelationshipGroupCursor( this ) );
        }

        try
        {
            return relationshipGroupCursor;
        }
        finally
        {
            relationshipGroupCursor = null;
        }
    }

    public void accept( DefaultRelationshipGroupCursor cursor )
    {
        if ( relationshipGroupCursor != null )
        {
            relationshipGroupCursor.release();
        }
        relationshipGroupCursor = cursor;
    }

    @Override
    public DefaultNodeValueIndexCursor allocateNodeValueIndexCursor()
    {
        if ( nodeValueIndexCursor == null )
        {
            return trace( new DefaultNodeValueIndexCursor( this ) );
        }

        try
        {
            return nodeValueIndexCursor;
        }
        finally
        {
            nodeValueIndexCursor = null;
        }
    }

    public void accept( DefaultNodeValueIndexCursor cursor )
    {
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
        }
        nodeValueIndexCursor = cursor;
    }

    @Override
    public DefaultNodeLabelIndexCursor allocateNodeLabelIndexCursor()
    {
        if ( nodeLabelIndexCursor == null )
        {
            return trace( new DefaultNodeLabelIndexCursor( this ) );
        }

        try
        {
            return nodeLabelIndexCursor;
        }
        finally
        {
            nodeLabelIndexCursor = null;
        }
    }

    public void accept( DefaultNodeLabelIndexCursor cursor )
    {
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
        }
        nodeLabelIndexCursor = cursor;
    }

    @Override
    public DefaultNodeExplicitIndexCursor allocateNodeExplicitIndexCursor()
    {
        if ( nodeExplicitIndexCursor == null )
        {
            return trace( new DefaultNodeExplicitIndexCursor( this ) );
        }

        try
        {
            return nodeExplicitIndexCursor;
        }
        finally
        {
            nodeExplicitIndexCursor = null;
        }
    }

    public void accept( DefaultNodeExplicitIndexCursor cursor )
    {
        if ( nodeExplicitIndexCursor != null )
        {
            nodeExplicitIndexCursor.release();
        }
        nodeExplicitIndexCursor = cursor;
    }

    @Override
    public DefaultRelationshipExplicitIndexCursor allocateRelationshipExplicitIndexCursor()
    {
        if ( relationshipExplicitIndexCursor == null )
        {
            return trace( new DefaultRelationshipExplicitIndexCursor( new DefaultRelationshipScanCursor( null ), this ) );
        }

        try
        {
            return relationshipExplicitIndexCursor;
        }
        finally
        {
            relationshipExplicitIndexCursor = null;
        }
    }

    public void accept( DefaultRelationshipExplicitIndexCursor cursor )
    {
        if ( relationshipExplicitIndexCursor != null )
        {
            relationshipExplicitIndexCursor.release();
        }
        relationshipExplicitIndexCursor = cursor;
    }

    public void release()
    {
        if ( nodeCursor != null )
        {
            nodeCursor.release();
            nodeCursor = null;
        }
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.release();
            relationshipScanCursor = null;
        }
        if ( relationshipTraversalCursor != null )
        {
            relationshipTraversalCursor.release();
            relationshipTraversalCursor = null;
        }
        if ( propertyCursor != null )
        {
            propertyCursor.release();
            propertyCursor = null;
        }
        if ( relationshipGroupCursor != null )
        {
            relationshipGroupCursor.release();
            relationshipGroupCursor = null;
        }
        if ( nodeValueIndexCursor != null )
        {
            nodeValueIndexCursor.release();
            nodeValueIndexCursor = null;
        }
        if ( nodeLabelIndexCursor != null )
        {
            nodeLabelIndexCursor.release();
            nodeLabelIndexCursor = null;
        }
        if ( nodeExplicitIndexCursor != null )
        {
            nodeExplicitIndexCursor.release();
            nodeExplicitIndexCursor = null;
        }
        if ( relationshipExplicitIndexCursor != null )
        {
            relationshipExplicitIndexCursor.release();
            relationshipExplicitIndexCursor = null;
        }
    }

    private <T extends AutoCloseablePlus> T trace( T closeable )
    {
        if ( DEBUG_CLOSING )
        {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            closeables.add( new CloseableStacktrace( closeable, Arrays.copyOfRange( stackTrace, 2, stackTrace.length ) ) );
        }
        return closeable;
    }

    void assertClosed()
    {
        if ( DEBUG_CLOSING )
        {
            for ( CloseableStacktrace c : closeables )
            {
                c.assertClosed();
            }
            closeables.clear();
        }
    }

    static class CloseableStacktrace
    {
        private final AutoCloseablePlus c;
        private final StackTraceElement[] stackTrace;

        CloseableStacktrace( AutoCloseablePlus c, StackTraceElement[] stackTrace )
        {
            this.c = c;
            this.stackTrace = stackTrace;
        }

        void assertClosed()
        {
            if ( !c.isClosed() )
            {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                PrintStream printStream = new PrintStream( out );

                for ( StackTraceElement traceElement : stackTrace )
                {
                    printStream.println( "\tat " + traceElement );
                }
                printStream.println();
                throw new IllegalStateException( format( "Closeable %s was not closed!\n%s", c, out.toString() ) );
            }
        }
    }
}
