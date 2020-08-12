package org.apache.flink.api.connector.sink;

import java.util.List;

/**
 * An interface for a Committer which is responsible for finalizing committables and making them
 * visible to external systems.
 */
public interface Committer<IN> {
	/**
	 * Finalizes the given committables, making them visible to the external systems.
	 *
	 * <p>The commit logic should be idempotent. This method might be called again if the failure happens
	 * while committing, resulting in some of the committables being committed in a previous call.
	 */
	void commit(List<IN> committables) throws Exception;

	/**
	 * Method for cleaning up any intermittent state.
	 *
	 * <p>It will be called upon a failure. Best-effort only.
	 */
	void cleanUp(List<IN> committables) throws Exception;
}
