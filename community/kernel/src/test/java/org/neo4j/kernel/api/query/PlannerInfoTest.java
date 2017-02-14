package org.neo4j.kernel.api.query;

import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class PlannerInfoTest
{
    @Test
    public void plannerInfoShouldBeInSmallCase() throws Exception
    {
        // given
        PlannerInfo plannerInfo = new PlannerInfo( "PLANNER", "RUNTIME", emptyList() );

        // then
        assertThat( plannerInfo.planner(), is( "planner" ) );
        assertThat( plannerInfo.runtime(), is( "runtime" ) );
    }
}
