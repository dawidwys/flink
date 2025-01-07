package org.apache.flink.table.operations;

import org.apache.flink.table.functions.FunctionDefinition;

public interface SerializationContext {
    String serializeInlineFunction(FunctionDefinition functionDefinition);
}
