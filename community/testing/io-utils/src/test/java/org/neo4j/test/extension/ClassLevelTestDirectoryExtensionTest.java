package org.neo4j.test.extension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.rule.TestDirectory;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class ClassLevelTestDirectoryExtensionTest
{
    @Inject
    FileSystemAbstraction fs;
    @Inject
    TestDirectory testDirectory;
    private StoreChannel channel;

    @BeforeAll
    void setUp() throws IOException
    {
        File file = testDirectory.createFile( "f" );
        channel = fs.write( file );
    }

    @AfterAll
    void tearDown() throws IOException
    {
        channel.close();
    }

    @RepeatedTest( 10 )
    void writeToChannelManyTimes() throws IOException
    {
        // This will fail if the test directory is not initialised,
        // or if the file is deleted by the clearing of the test directory,
        // in between the runs.
        channel.write( ByteBuffer.allocate( 1 ) );
    }
}
