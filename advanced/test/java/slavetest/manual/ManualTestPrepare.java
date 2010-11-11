package slavetest.manual;

import org.apache.commons.io.FileUtils;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class ManualTestPrepare
{
    public static void main( String[] args ) throws Exception
    {
        FileUtils.deleteDirectory( ManualTest1.PATH.getParentFile() );
        new EmbeddedGraphDatabase( ManualTest1.PATH.getAbsolutePath() ).shutdown();
        FileUtils.copyDirectory( ManualTest1.PATH, ManualTest2.PATH );
        FileUtils.deleteDirectory( ManualZooKeepers.PATH );
    }
}
