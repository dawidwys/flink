package org.apache.flink.connector.source;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;

/**
 * Tells sources created from {@link TestValuesTableFactory} how they should behave after producing
 * all data.
 */
public enum FinishingLogic {
    INFINITE,
    FINITE;
}
