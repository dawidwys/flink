package org.apache.flink.table.api;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.operations.SerializationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Internal
public class PlannerConfigs {

    public static PlannerConfig of(PlannerConfig... configs) {
        return new ChainConfig(List.of(configs));
    }

    public static class SerializationConfig implements PlannerConfig {
        private final SerializationContext context;

        public SerializationConfig(SerializationContext context) {
            this.context = context;
        }

        public SerializationContext getContext() {
            return context;
        }
    }

    private static final class ChainConfig implements PlannerConfig {

        private final Map<Class<? extends PlannerConfig>, PlannerConfig> configs = new HashMap<>();

        private ChainConfig(List<PlannerConfig> configs) {
            for (PlannerConfig config : configs) {
                if (config instanceof ChainConfig) {
                    this.configs.putAll(((ChainConfig) config).configs);
                } else {
                    this.configs.put(config.getClass(), config);
                }
            }
        }

        @Override
        public <T extends PlannerConfig> Optional<T> unwrap(Class<T> type) {
            return Optional.ofNullable(configs.get(type)).map(type::cast);
        }
    }
}
