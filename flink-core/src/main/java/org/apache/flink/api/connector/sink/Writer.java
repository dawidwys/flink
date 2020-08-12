package org.apache.flink.api.connector.sink;

import java.util.List;

/**
 * An interface for a writer that writes to a durable storage and produces a committable handles to
 * the persisted results.
 *
 * @param <IN> Incoming element
 * @param <CommT> Committable handle that can be committed by the {@link Committer}
 * @param <StateT> Internal state
 */
public interface Writer<IN, CommT, StateT> {


	void write(IN element, Context<CommT> ctx) throws Exception;

	/**
	 * Persists all remaining transient state to a durable storage. After this method
	 * there will be no more incoming new elements.
	 *
	 * <b>IMPORTANT:</b> In case of a STREAM style execution it means this method should drain any
	 * Flink style. Any left over state will not be committed.
	 */
	void persist(CommitContext<CommT> ctx) throws Exception;

	/**
	 * Snapshots the current state of the Writer. It can produce committables which will be committed when
	 * the distributed snapshot succeeds. In comparison to the {@link #persist(CommitContext)} it can produce
	 * a state that will not be committed in the external storage, but will be resotred upon failure.
	 *
	 * <p>The snapshotted, but not committed state can be redistributed to multiple Writers when restoring.
	 *
	 * <b>Note:</b> This method will never be called in a BATCH style execution.
	 *
	 * @return The state to include in the snapshot.
	 */
	List<StateT> snapshot(CommitContext<CommT> output);

	/**
	 * Exposes a way to submit committables.
	 */
	interface CommitContext<CommT> {
		/**
		 * Submits a committable. Can be called multiple times.
		 *
		 * <p>The actual committing does not happen immediately.
		 *
		 * <b>Implementation note:</b>In case of STREAM execution it will perform a distributed commit,
		 * when a checkpoint completes. In case of BATCH the commit will happen after the whole job
		 * finishes succesfully.
		 */
		void commit(CommT committable);
	}

	/**
	 * Exposes additional required features, when writing the results. E.g. it allows registering
	 * callbacks with a delay.
	 */
	interface Context<CommT> extends CommitContext<CommT> {
		/**
		 * Registers a {@link Callback} that will be invoked after a delay in processing time.
		 * The callback will never be executed concurrently with other methods of the {@link Writer}.
		 * Therefore it is safe to mutate the state of the writer inside of the callback.
		 */
		void registerCallback(Callback<CommT> callback, long delay);
	}

	/**
	 * A callback that can be registered with {@link Context#registerCallback(Callback, long)}.
	 * The callback will never be executed concurrently with other methods of the {@link Writer}.
	 */
	interface Callback<CommT> {
		void apply(Context<CommT> ctx, Context<CommT> output);
	}

	/**
	 * Method for cleaning up any intermittent state.
	 *
	 * <p>It will be called when the writer fails, either when writing or sending a committable.
	 * It is only a best effort clean up, which is not necessary for correctness.
	 */
	void cleanUp();
}
