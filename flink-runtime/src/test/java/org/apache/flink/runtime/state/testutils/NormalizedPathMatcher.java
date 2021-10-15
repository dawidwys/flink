/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.testutils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLoaderTest;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** Due to canonicalization, paths need to be renormalized before comparison. */
public class NormalizedPathMatcher extends TypeSafeMatcher<Path> {
    private final Path reNormalizedExpected;

    private NormalizedPathMatcher(Path expected) {
        this.reNormalizedExpected = expected == null ? null : new Path(expected.toString());
    }

    public static Matcher<Path> normalizedPath(Path expected) {
        return new NormalizedPathMatcher(expected);
    }

    @Override
    protected boolean matchesSafely(Path actual) {
        if (reNormalizedExpected == null) {
            return actual == null;
        }

        Path reNormalizedActual = new Path(actual.toString());
        return reNormalizedExpected.equals(reNormalizedActual);
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(reNormalizedExpected);
    }
}
