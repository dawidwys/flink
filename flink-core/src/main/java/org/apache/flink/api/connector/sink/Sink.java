package org.apache.flink.api.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.List;

public interface Sink<IN, CommT, WriterState> extends Serializable {
	/**
	 * Creates a new {@link Writer}.
	 */
	Writer<IN, CommT, WriterState> createWriter(WriterInitContext<CommT> context) throws Exception;

	/**
	 * Creates a {@link Writer} with a restored state. This method may produce committables
	 * via {@link WriterInitContext#commit(Object)}. The state might be redistributed from
	 * a previous run.
	 */
	Writer<IN, CommT, WriterState> restoreWriter(
		WriterInitContext<CommT> context,
		List<WriterState> checkpoint) throws Exception;

	/**
	 * Creates a new {@link Committer}.
	 */
	Committer<CommT> createCommitter() throws Exception;

	/**
	 * Creates a serializer for the committable shipped between the {@link Writer} and {@link Committer}.
	 */
	SimpleVersionedSerializer<CommT> getCommittableSerializer() throws Exception;

	/**
	 * Creates a serializer for the intermittent state of the {@link Writer}.
	 */
	SimpleVersionedSerializer<WriterState> getWriterStateSerializer() throws Exception;

	interface WriterInitContext<CommT> {

		/**
		 * @return The metric group this source belongs to.
		 */
		MetricGroup metricGroup();

		/**
		 * Submits a committable. Can be called multiple times.
		 *
		 * <p>The actual committing does not happen immediately.
		 *
		 * <b>Implementation note:</b>In case of STREAM execution it will perform a distributed commit,
		 * when a checkpoint completes. In case of BATCH the commit will happen after the whole job
		 * finishes succesfully.
		 */
		void commit(CommT committables);
	}
}
