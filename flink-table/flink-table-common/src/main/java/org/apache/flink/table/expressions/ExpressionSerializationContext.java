package org.apache.flink.table.expressions;

import org.apache.flink.table.functions.FunctionDefinition;

public interface ExpressionSerializationContext {
    String serializeInlineFunction(FunctionDefinition functionDefinition);
}
