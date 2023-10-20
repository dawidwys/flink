package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.Collections;
import java.util.List;

/** Restore tests for {@link StreamExecCalc}. */
public class CalcRestoreTest extends RestoreTestBase {

    public CalcRestoreTest() {
        super(StreamExecCalc.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(CalcTestProgram.SIMPLE_CALC);
    }
}
