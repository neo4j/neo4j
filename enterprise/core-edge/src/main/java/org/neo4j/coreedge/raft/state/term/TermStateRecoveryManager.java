package org.neo4j.coreedge.raft.state.term;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.state.StateRecoveryManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

public class TermStateRecoveryManager extends StateRecoveryManager
{
    private final InMemoryTermState.InMemoryTermStateMarshal marshal;

    public TermStateRecoveryManager( FileSystemAbstraction fileSystem, InMemoryTermState.InMemoryTermStateMarshal marshal )
    {
        super( fileSystem );
        this.marshal = marshal;
    }

    @Override
    protected long getOrdinalOfLastRecord( File file ) throws IOException
    {
        return readLastEntryFrom( fileSystem, file ).currentTerm();
    }

    public InMemoryTermState readLastEntryFrom( FileSystemAbstraction fileSystemAbstraction, File file )
            throws IOException
    {
        final ByteBuffer workingBuffer = ByteBuffer.allocate( 2_000_000 );

        final StoreChannel channel = fileSystemAbstraction.open( file, "rw" );
        channel.read( workingBuffer );
        workingBuffer.flip();

        InMemoryTermState result = new InMemoryTermState( );
        InMemoryTermState lastRead;

        while ( (lastRead = marshal.unmarshal( workingBuffer )) != null )
        {
            result = lastRead;
        }

        return result;
    }
}
