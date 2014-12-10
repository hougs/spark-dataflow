/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PObject;

/**
 * What should implement this interface?
 */
public interface EvaluationResult extends PipelineResult {
    /**
     *
     * @param pcollection
     * @param <T>
     * @return
     */
  <T> Iterable<T> get(PCollection<T> pcollection);

    /**
     * Why is this never used?
     * @param pobject
     * @param <T>
     * @return
     */
  <T> T get(PObject<T> pobject);

    /**
     * What is named?
     * @param named
     * @param resultType
     * @param <T>
     * @return
     */
  <T> T getAggregatorValue(String named, Class<T> resultType);
}
