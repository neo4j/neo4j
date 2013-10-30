package org.neo4j.consistency.checking.full;

import org.junit.Test;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.consistency.checking.RecordCheckTestBase.inUse;
import static org.neo4j.consistency.checking.RecordCheckTestBase.notInUse;

public class LabelScanStoreCheckTaskTest
{
    @Test
    public void shouldReportNodeNotInUse() throws Exception
    {
        // given
        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        LabelScanStoreCheckTask.NodeRecordCheck check = new LabelScanStoreCheckTask.NodeRecordCheck();
        NodeRecord node = notInUse( new NodeRecord( 42, 0, 0 ) );

        // when
        check.checkReference( null, node, engineFor( report ), null );

        // then
        verify( report ).nodeNotInUse( node );
    }

    @Test
    public void shouldRemainSilentWhenEverythingIsInOrder() throws Exception
    {
        // given
        ConsistencyReport.LabelScanConsistencyReport report =
                mock( ConsistencyReport.LabelScanConsistencyReport.class );
        LabelScanStoreCheckTask.NodeRecordCheck check = new LabelScanStoreCheckTask.NodeRecordCheck();
        NodeRecord node = inUse( new NodeRecord( 42, 0, 0 ) );

        // when
        check.checkReference( null, node, engineFor( report ), null );

        // then
        verifyNoMoreInteractions( report );
    }

    private Engine engineFor( ConsistencyReport.LabelScanConsistencyReport report )
    {
        Engine engine = mock( Engine.class );
        when( engine.report() ).thenReturn( report );
        return engine;
    }

    interface Engine extends CheckerEngine<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport>
    {
    }
}
