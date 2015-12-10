package org.neo4j.coreedge.raft.replication.id;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.coreedge.raft.replication.id.InMemoryIdAllocationState.Serializer
        .NUMBER_OF_BYTES_PER_WRITE;

public class IdAllocationStoreRecoveryManager
{
    private static final long EMPTY = -1;

    private final FileSystemAbstraction fileSystem;

    public enum RecoveryStatus
    {
        NEW, RECOVERABLE, UNRECOVERABLE;

        private File active;

        public File getActive()
        {
            return active;
        }

        public void setActive( File active )
        {
            this.active = active;
        }
    }

    public IdAllocationStoreRecoveryManager( final FileSystemAbstraction fsa )
    {
        this.fileSystem = fsa;
    }

    public File recover( File fileA, File fileB ) throws IOException
    {
        assert fileA != null && fileB != null;

        ensureExists( fileA );
        ensureExists( fileB );

        RecoveryStatus recoveryStatus;

        long a = getLogIndex( fileA );
        long b = getLogIndex( fileB );

        if ( a > b )
        {
            RecoveryStatus.RECOVERABLE.setActive( fileA );
            recoveryStatus = RecoveryStatus.RECOVERABLE;
        }
        else if ( a < b )
        {
            RecoveryStatus.RECOVERABLE.setActive( fileB );
            recoveryStatus = RecoveryStatus.RECOVERABLE;
        }
        else if ( a == b && a == EMPTY )
        {
            recoveryStatus = RecoveryStatus.NEW;
        }
        else
        {
            recoveryStatus = RecoveryStatus.UNRECOVERABLE;
        }

        File toReturn = null;

        switch ( recoveryStatus )
        {
            case NEW:
                toReturn = fileA;
                break;

            case RECOVERABLE:
                toReturn = trimGarbage( recoveryStatus.getActive() );
                break;

            case UNRECOVERABLE:
                throw new RuntimeException( "Developer Alistair says a lot of things" );
        }

        return toReturn;
    }

    private void ensureExists( File file ) throws IOException
    {
        if ( !fileSystem.fileExists( file ) )
        {
            fileSystem.mkdirs( file.getParentFile() );
            fileSystem.create( file );
        }
    }

    private File trimGarbage( File storeFile ) throws IOException
    {
        long fileSize = fileSystem.getFileSize( storeFile );
        long extraneousBytes = fileSize % NUMBER_OF_BYTES_PER_WRITE;
        if ( extraneousBytes != 0 )
        {
            fileSystem.truncate( storeFile, fileSize - extraneousBytes );
        }

        return storeFile;
    }

    private long getLogIndex( File storeFile ) throws IOException
    {
        long newPosition = beginningOfLastCompleteEntry( storeFile );

        if ( newPosition < 0 )
        {
            return newPosition;
        }

        ByteBuffer buffer = ByteBuffer.allocate(
                NUMBER_OF_BYTES_PER_WRITE );

        StoreChannel channel = fileSystem.open( storeFile, "r" );

        channel.position( newPosition );

        channel.read( buffer );

        buffer.flip();

        InMemoryIdAllocationState inMemoryIdAllocationState =
                new InMemoryIdAllocationState.Serializer().deserialize( buffer );

        channel.close();

        return inMemoryIdAllocationState.logIndex();
    }

    /*
       * This method sets the position of the current channel to point to the beginning of the last complete entry.
       * It integer-divides the file size by the entry size (thus finding the number of complete entries), it then
       * subtracts one (which is the index of the next-to-last entry) and then multiplies by the entry size, which
       * finds the end of the next-to-last entry and therefore the beginning of the last complete entry.
       * It is assumed that the currentChannel contains at least one complete entry.
       */
    private long beginningOfLastCompleteEntry( File storeFile ) throws IOException
    {
        if ( storeFile == null )
        {
            return -1;
        }
        if ( fileSystem.getFileSize( storeFile ) < NUMBER_OF_BYTES_PER_WRITE )
        {
            return -1;
        }

        long fileSize = fileSystem.getFileSize( storeFile );
        long positionOfLastCompleteEntry =
                ((fileSize / NUMBER_OF_BYTES_PER_WRITE) - 1)
                        * NUMBER_OF_BYTES_PER_WRITE;
        return positionOfLastCompleteEntry;
    }
}
