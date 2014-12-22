/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark.transform;

import com.cloudera.dataflow.spark.EvaluationResult;
import com.cloudera.dataflow.spark.aggregate.BroadcastHelper;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;
import java.util.Set;

/**
 * This context is available to every transformation when that transformation gets evaluated. The
 * actions of PTransforms are translated to
 */
public class EvaluationContext implements EvaluationResult {
  private final JavaSparkContext mJSparkContext;
  private final Pipeline mPipeline;
  private final SparkRuntimeContext mRuntimeContext;
  private final Map<PValue, JavaRDDLike> mRdds = Maps.newHashMap();
  private final Set<PValue> mMultiReads = Sets.newHashSet();
  private final Map<String, BroadcastHelper<?>> mSideInputs = Maps.newHashMap();

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.mJSparkContext = jsc;
    this.mPipeline = pipeline;
    this.mRuntimeContext = new SparkRuntimeContext(jsc, pipeline);
  }

  JavaSparkContext getSparkContext() {
    return mJSparkContext;
  }

  Pipeline getPipeline() {
    return mPipeline;
  }

  SparkRuntimeContext getRuntimeContext() {
    return mRuntimeContext;
  }

  <I extends PInput> I getInput(PTransform<I, ?> transform) {
    return (I) mPipeline.getInput(transform);
  }

  <O extends POutput> O getOutput(PTransform<?, O> transform) {
    return (O) mPipeline.getOutput(transform);
  }

  void setOutputRDD(PTransform transform, JavaRDDLike rdd) {
    mRdds.put((PValue) getOutput(transform), rdd);
  }

  JavaRDDLike getRDD(PValue pvalue) {
    JavaRDDLike rdd = mRdds.get(pvalue);
    if (mMultiReads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      rdd.rdd().cache();
    } else {
      mMultiReads.add(pvalue);
    }
    return rdd;
  }

  void setRDD(PValue pvalue, JavaRDDLike rdd) {
    mRdds.put(pvalue, rdd);
  }

  JavaRDDLike getInputRDD(PTransform transform) {
    return getRDD((PValue) mPipeline.getInput(transform));
  }

  CoderRegistry getCoderRegistry() {
    return mPipeline.getCoderRegistry();
  }

  @Override
  public <T> T getAggregatorValue(String named, Class<T> resultType) {
    return mRuntimeContext.getAggregatorValue(named, resultType);
  }

  @Override
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    return getRDD(pcollection).collect();
  }

  @Override
  public <T> T get(TupleTag<T> tupleTag) {
    return null;
  }

  /**
   * Broadcasts the specified side input.
   *
   * @param <T> The type of the underlying object being broadcast.
   * @return A broadcast helper to assist in deserializing the broadcast side input.
   */
  <T> BroadcastHelper<T> broadcast(PInput input, Coder coder) {
     Broadcast<byte[]> bcast = mJSparkContext.broadcast(CoderHelpers.toByteArray(input,
     coder));
    return new BroadcastHelper<T>(bcast, coder);
  }

  <T, WT> void setSideInput(PCollectionView<T, WT> view, PInput input, Coder<WT> coder) {
      mSideInputs.put(view.getTagInternal().getId(), broadcast(input, coder));
  }

  <T> T getSideInput(PCollectionView<T, ?> view) {
    String id = view.getTagInternal().getId();
    if (!mSideInputs.containsKey(id)) {
      throw new IllegalArgumentException( "calling sideInput() with unknown view; did you forget" +
          " to pass the view in ParDo.withSideInputs()?");
    }
    return (T) mSideInputs.get(id).getValue();
  }

}
