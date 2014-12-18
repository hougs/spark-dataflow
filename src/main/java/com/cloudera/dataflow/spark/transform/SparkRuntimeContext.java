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

import com.cloudera.dataflow.spark.aggregate.AggAccumParam;
import com.cloudera.dataflow.spark.aggregate.BroadcastHelper;
import com.cloudera.dataflow.spark.aggregate.NamedAggregators;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The SparkRuntimeContext allows us to define aggregators and side inputs, and access them
 * when processing transformations on our pipeline. The SparkPipelineContext applies for the
 * entire execution of the pipeline. we can instatiate aggregators and broadcast vars,
 * access their end results, and increment or access trhem in sub transformations via the single
 * transform context.
 */
public class SparkRuntimeContext implements Serializable {
  /**
   * An accumulator that is a map from names to aggregators.
   */
  private Accumulator<NamedAggregators> accum;
  /**
   * Map from names to dataflow defined aggregators.
   */
  private Map<String, Aggregator> mAggregators = new HashMap<>();
  /** undelrying spark context */
  private JavaSparkContext mJSparkContext;
  /** Map from TupleTags references to Broadcast variables.*/
  private Map<TupleTag<?>, BroadcastHelper<?>> mSideInputs = new HashMap<>();

  public SparkRuntimeContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.mJSparkContext = jsc;
    this.accum = jsc.accumulator(new NamedAggregators(), new AggAccumParam());
  }

  /**
   * Retrieves corresponding value of an aggregator.
   *
   * @param aggregatorName Name of the aggregator to retrieve the value of.
   * @param typeClass      Type class of value to be retrieved.
   * @param <T>            Type of object to be returned.
   * @return The value of the aggregator.
   */
  public <T> T getAggregatorValue(String aggregatorName, Class<T> typeClass) {
    return accum.value().getValue(aggregatorName, typeClass);
  }

  public synchronized PipelineOptions getPipelineOptions() {
    throw new UnsupportedOperationException("getPipelineOptions is not yet supported.");
  }

  /**
   * Creates and aggregator and associates it with the specified name.
   *
   * @param named Name of aggregator.
   * @param sfunc Serializable function used in aggregation.
   * @param <In>  Type of inputs to aggregator.
   * @param <Out> Type of aggregator outputs.
   * @return Specified aggregator
   */
  public synchronized <In, Out> Aggregator<In> createAggregator(
      String named,
      SerializableFunction<Iterable<In>, Out> sfunc) {
    Aggregator aggregator = mAggregators.get(named);
    if (aggregator == null) {
      NamedAggregators.SerFunctionState<In, Out> state = new NamedAggregators
          .SerFunctionState<>(sfunc);
      accum.add(new NamedAggregators(named, state));
      aggregator = new SparkAggregator(state);
      mAggregators.put(named, aggregator);
    }
    return aggregator;
  }

  /**
   * Creates and aggregator and associates it with the specified name.
   *
   * @param named     Name of aggregator.
   * @param combineFn Combine function used in aggregation.
   * @param <In>      Type of inputs to aggregator.
   * @param <Out>     Type of aggregator outputs.
   * @return Specified aggregator
   */
  public synchronized <In, Inter, Out> Aggregator<In> createAggregator(
      String named,
      Combine.CombineFn<? super In, Inter, Out> combineFn) {
    Aggregator aggregator = mAggregators.get(named);
    if (aggregator == null) {
      NamedAggregators.CombineFunctionState<? super In, Inter, Out> state = new NamedAggregators
          .CombineFunctionState<>(combineFn);
      accum.add(new NamedAggregators(named, state));
      aggregator = new SparkAggregator(state);
      mAggregators.put(named, aggregator);
    }
    return aggregator;
  }

  /**
   * Broadcasts the specified side input.
   *
   * @param view The side input to broadcast.
   * @param <T> The type of the underlying object being broadcast.
   * @return A broadcast helper to assist in deserializing the broadcast side input.
   */
  <T> BroadcastHelper<T> broadcast(PCollectionView<T, ?> view, CoderRegistry registry) {
    TupleTag<?> tag = view.getTagInternal();
    T inputToBroadcast = view.fromIterableInternal((Iterable<WindowedValue<?>>) view.get
        (tag));
    Broadcast<byte[]> bcast = mJSparkContext.broadcast(CoderHelpers.toByteArray(inputToBroadcast,
        registry.getDefaultCoder(tag)));
    return new BroadcastHelper<>(bcast, registry.getDefaultCoder(tag)));
  }

  void setSideInputs(List<PCollectionView<?, ?>> views, CoderRegistry registry) {
    for (PCollectionView<?, ?> view : views) {
      mSideInputs.put(view.getTagInternal(), broadcast(view, registry));
    }
  }

  public <T> T getSideInput(PCollectionView<T, ?> view) {
    return (T) mSideInputs.get(view).getValue();
  }

  /**
   * Initialize spark aggregators exactly once.
   *
   * @param <In> Type of element fed in to aggregator.
   */
  private static class SparkAggregator<In> implements Aggregator<In> {
    private final NamedAggregators.State<In, ?, ?> state;

    public SparkAggregator(NamedAggregators.State<In, ?, ?> state) {
      this.state = state;
    }

    @Override
    public void addValue(In elem) {
      state.update(elem);
    }
  }
}
