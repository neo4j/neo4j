package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;

public abstract class DataWriter
{
    public static final DataWriter VOID = new DataWriter()
    {
        @Override
        public void write( ChannelBuffer buffer )
        {
        }
    };

    public static final class WriteInt extends DataWriter
    {
        private final int value;

        public WriteInt( int value )
        {
            this.value = value;
        }

        @Override
        public void write( ChannelBuffer buffer )
        {
            buffer.writeInt( value );
        }
    }

    public static final class WriteLong extends DataWriter
    {
        private final long value;

        public WriteLong( long value )
        {
            this.value = value;
        }

        @Override
        public void write( ChannelBuffer buffer )
        {
            buffer.writeLong( value );
        }
    }

    public static final class WriteString extends DataWriter
    {
        private final String string;

        public WriteString( String string )
        {
            this.string = string;
        }

        @Override
        public void write( ChannelBuffer buffer )
        {
            CommunicationUtils.writeString( string, buffer, false );
        }
    }

    public static final class WriteLockResult extends DataWriter
    {
        private final LockResult result;

        public WriteLockResult( LockResult result )
        {
            this.result = result;
        }

        @Override
        public void write( ChannelBuffer buffer )
        {
            CommunicationUtils.writeLockResult( result, buffer );
        }
    }

    public static final class WriteIdAllocation extends DataWriter
    {
        private final IdAllocation result;

        public WriteIdAllocation( IdAllocation result )
        {
            this.result = result;
        }

        @Override
        public void write( ChannelBuffer buffer )
        {
            CommunicationUtils.writeIdAllocation( result, buffer );
        }
    }

    public static final class WriteIdArray extends DataWriter
    {
        private final long[] ids;

        public WriteIdArray( long[] ids )
        {
            this.ids = ids;
        }

        @Override
        public void write( ChannelBuffer buffer )
        {
            CommunicationUtils.writeIdArray( ids, buffer );
        }
    }

    public abstract void write( ChannelBuffer buffer );
}
