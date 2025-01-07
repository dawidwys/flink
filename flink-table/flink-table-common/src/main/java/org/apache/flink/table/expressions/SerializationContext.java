package org.apache.flink.table.expressions;

import org.apache.flink.table.functions.FunctionDefinition;

public interface SerializationContext {
    String serializeInlineFunction(FunctionDefinition functionDefinition);
}
