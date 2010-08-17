package slavetest;

import java.io.File;

import org.neo4j.kernel.impl.util.DumpLogicalLog;

public class DumpLogs
{
    public static void main( String[] args ) throws Exception
    {
        for ( File file : new File( "target/havar/dbs/1" ).listFiles() )
        {
            if ( file.getName().contains( "logical" ) && !file.getName().endsWith( ".active" ) )
            {
                System.out.println( "\n=== " + file.getPath() + " ===" );
                if ( file.getName().contains( ".ptx_" ) || file.getName().contains( ".tx_" ) )
                {
                    DumpLogicalLog.main( new String[] { "-single", file.getPath() } );
                }
                else
                {
                    DumpLogicalLog.main( new String[] { file.getPath() } );
                }
            }
        }
    }
}
