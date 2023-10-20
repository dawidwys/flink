package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

/** {@link TableTestProgram} definitions for testing {@link StreamExecCalc}. */
public class CalcTestProgram {

    static final TableTestProgram SIMPLE_CALC =
            TableTestProgram.of("simple-calc", "Simple calc with sources and sinks")
                    .runSql("INSERT INTO sink_t SELECT a + 1, b FROM t")
                    .setupTableSource("t")
                    .withSchema("a BIGINT", "b DOUBLE")
                    .withValuesBeforeRestore(Row.of(420L, 42.0))
                    .withValuesAfterRestore(Row.of(421L, 42.1))
                    .complete()
                    .setupTableSink("sink_t")
                    .withSchema("a BIGINT", "b DOUBLE")
                    .withValuesBeforeRestore(Row.of(421L, 42.0))
                    .withValuesAfterRestore(Row.of(421L, 42.0), Row.of(422L, 42.1))
                    .complete()
                    .build();
}
