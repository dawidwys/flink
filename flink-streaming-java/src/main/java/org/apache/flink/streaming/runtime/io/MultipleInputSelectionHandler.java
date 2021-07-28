/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This handler is mainly used for selecting the next available input index in {@link
 * StreamMultipleInputProcessor}.
 */
@Internal
public class MultipleInputSelectionHandler {
    // if we directly use Long.SIZE, calculation of allSelectedMask will overflow
    public static final int MAX_SUPPORTED_INPUT_COUNT = Long.SIZE - 1;

    @Nullable private final InputSelectable inputSelectable;

    private long selectedInputsMask = InputSelection.ALL.getInputMask();

    private final long allSelectedMask;

    private long availableInputsMask;

    private long notFinishedInputsMask;

    private long dataFinishedButNotPartition;

    public MultipleInputSelectionHandler(
            @Nullable InputSelectable inputSelectable, int inputCount) {
        checkSupportedInputCount(inputCount);
        this.inputSelectable = inputSelectable;
        this.allSelectedMask = (1L << inputCount) - 1;
        this.availableInputsMask = allSelectedMask;
        this.notFinishedInputsMask = allSelectedMask;
        this.dataFinishedButNotPartition = 0;
    }

    public static void checkSupportedInputCount(int inputCount) {
        checkArgument(
                inputCount <= MAX_SUPPORTED_INPUT_COUNT,
                "Only up to %s inputs are supported at once, while encountered %s",
                MAX_SUPPORTED_INPUT_COUNT,
                inputCount);
    }

    public void updateStatus(DataInputStatus inputStatus, int inputIndex) throws IOException {
        switch (inputStatus) {
            case MORE_AVAILABLE:
                checkState(checkBitMask(availableInputsMask, inputIndex));
                break;
            case NOTHING_AVAILABLE:
                availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
                break;
            case END_OF_DATA:
                dataFinishedButNotPartition = setBitMask(dataFinishedButNotPartition, inputIndex);
                break;
            case END_OF_INPUT:
                dataFinishedButNotPartition = unsetBitMask(dataFinishedButNotPartition, inputIndex);
                notFinishedInputsMask = unsetBitMask(notFinishedInputsMask, inputIndex);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported inputStatus = " + inputStatus);
        }
    }

    public DataInputStatus calculateOverallStatus(DataInputStatus updatedStatus)
            throws IOException {
        if (updatedStatus == DataInputStatus.MORE_AVAILABLE) {
            return DataInputStatus.MORE_AVAILABLE;
        }

        if (areAllInputsFinished()) {
            return DataInputStatus.END_OF_INPUT;
        }

        if (updatedStatus == DataInputStatus.END_OF_DATA && areAllDataInputsFinished()) {
            return DataInputStatus.END_OF_DATA;
        }

        if (isAnyInputAvailable()) {
            return DataInputStatus.MORE_AVAILABLE;
        } else {
            long selectedNotFinishedInputMask = selectedInputsMask & notFinishedInputsMask;
            if (selectedNotFinishedInputMask == 0) {
                throw new IOException(
                        "Can not make a progress: all selected inputs are already finished");
            }
            return DataInputStatus.NOTHING_AVAILABLE;
        }
    }

    void nextSelection() {
        if (inputSelectable == null || areAllDataInputsFinished()) {
            selectedInputsMask = InputSelection.ALL.getInputMask();
        } else if (dataFinishedButNotPartition != 0) {
            selectedInputsMask =
                    (inputSelectable.nextSelection().getInputMask() | dataFinishedButNotPartition)
                            & allSelectedMask;
        } else {
            selectedInputsMask = inputSelectable.nextSelection().getInputMask();
        }
    }

    int selectNextInputIndex(int lastReadInputIndex) {
        return InputSelection.fairSelectNextIndex(
                selectedInputsMask,
                availableInputsMask & notFinishedInputsMask,
                lastReadInputIndex);
    }

    boolean shouldSetAvailableForAnotherInput() {
        return (selectedInputsMask & allSelectedMask & ~availableInputsMask) != 0;
    }

    void setAvailableInput(int inputIndex) {
        availableInputsMask = setBitMask(availableInputsMask, inputIndex);
    }

    void setUnavailableInput(int inputIndex) {
        availableInputsMask = unsetBitMask(availableInputsMask, inputIndex);
    }

    boolean isAnyInputAvailable() {
        return (selectedInputsMask & availableInputsMask & notFinishedInputsMask) != 0;
    }

    boolean isInputSelected(int inputIndex) {
        return checkBitMask(selectedInputsMask, inputIndex);
    }

    public boolean isInputFinished(int inputIndex) {
        return !checkBitMask(notFinishedInputsMask, inputIndex);
    }

    public boolean areAllInputsFinished() {
        return notFinishedInputsMask == 0;
    }

    public boolean areAllDataInputsFinished() {
        return ((dataFinishedButNotPartition | ~notFinishedInputsMask) & allSelectedMask)
                == allSelectedMask;
    }

    long setBitMask(long mask, int inputIndex) {
        return mask | 1L << inputIndex;
    }

    long unsetBitMask(long mask, int inputIndex) {
        return mask & ~(1L << inputIndex);
    }

    boolean checkBitMask(long mask, int inputIndex) {
        return (mask & (1L << inputIndex)) != 0;
    }
}
