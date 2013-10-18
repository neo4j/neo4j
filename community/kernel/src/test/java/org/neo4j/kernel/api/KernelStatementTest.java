package org.neo4j.kernel.api;

import org.junit.Test;

import org.neo4j.kernel.api.scan.LabelScanReader;
import org.neo4j.kernel.api.scan.LabelScanStore;
import org.neo4j.kernel.impl.api.IndexReaderFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KernelStatementTest
{
    @Test
    public void shouldCloseOpenedLabelScanReader() throws Exception
    {
        // given
        LabelScanStore scanStore = mock( LabelScanStore.class );
        LabelScanReader scanReader = mock( LabelScanReader.class );

        when( scanStore.newReader() ).thenReturn( scanReader );
        KernelStatement statement =
            new KernelStatement(
                mock( KernelTransactionImplementation.class ),
                mock( IndexReaderFactory.class ), scanStore, null, null, null, null );

        statement.acquire();

        // when
        LabelScanReader actualReader = statement.getLabelScanReader();

        // then
        assertEquals( scanReader, actualReader );

        // when
        statement.close();

        // then
        verify( scanStore ).newReader();
        verifyNoMoreInteractions( scanStore );

        verify( scanReader ).close();
        verifyNoMoreInteractions( scanReader );
    }
}
