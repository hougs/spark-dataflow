package com.cloudera.dataflow.spark.transform;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on RDDs.
 */
public class TransformTranslator {
  private static class FieldGetter {
    private Map<String, Field> fields;

    public FieldGetter(Class<?> clazz) {
      this.fields = Maps.newHashMap();
      for (Field f : clazz.getDeclaredFields()) {
        try {
          f.setAccessible(true);
          this.fields.put(f.getName(), f);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    public <T> T get(String fieldname, Object value) {
      try {
        return (T) fields.get(fieldname).get(value);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static TransformEvaluator<Flatten.FlattenPCollectionList> FLATTEN_PCOLLECTION_LIST =
      new TransformEvaluator<Flatten.FlattenPCollectionList>() {
        @Override
        public void evaluate(Flatten.FlattenPCollectionList transform, EvaluationContext context) {
          PCollectionList<?> pcs = (PCollectionList<?>) context.getPipeline().getInput(transform);
          JavaRDD[] rdds = new JavaRDD[pcs.size()];
          for (int i = 0; i < rdds.length; i++) {
            rdds[i] = (JavaRDD) context.getRDD(pcs.get(i));
          }
          JavaRDD rdd = context.getSparkContext().union(rdds);
          context.setOutputRDD(transform, rdd);
        }
      };

  private static TransformEvaluator<GroupByKey.GroupByKeyOnly> GBK = new
      TransformEvaluator<GroupByKey.GroupByKeyOnly>() {
    @Override
    public void evaluate(GroupByKey.GroupByKeyOnly transform, EvaluationContext context) {
      context.setOutputRDD(transform, fromPair(toPair(context.getInputRDD(transform)).groupByKey
          ()));
    }
  };

  private static FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);
  private static TransformEvaluator<Combine.GroupedValues> GROUPED = new
      TransformEvaluator<Combine.GroupedValues>() {
    @Override
    public void evaluate(Combine.GroupedValues transform, EvaluationContext context) {
      final Combine.KeyedCombineFn keyed = GROUPED_FG.get("fn", transform);
      context.setOutputRDD(transform, context.getInputRDD(transform).map(new Function() {
        @Override
        public Object call(Object input) throws Exception {
          KV<Object, Iterable> kv = (KV<Object, Iterable>) input;
          return KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue()));
        }
      }));
    }
  };

  private static JavaPairRDD toPair(JavaRDDLike rdd) {
    return rdd.mapToPair(new PairFunction() {
      @Override
      public Tuple2 call(Object o) throws Exception {
        KV kv = (KV) o;
        return new Tuple2(kv.getKey(), kv.getValue());
      }
    });
  }

  private static JavaRDDLike fromPair(JavaPairRDD rdd) {
    return rdd.map(new Function() {
      @Override
      public Object call(Object o) throws Exception {
        Tuple2 t2 = (Tuple2) o;
        return KV.of(t2._1(), t2._2());
      }
    });
  }


  private static TransformEvaluator<ParDo.Bound> PARDO = new TransformEvaluator<ParDo.Bound>() {
    @Override
    public void evaluate(ParDo.Bound transform, EvaluationContext context) {
      /**
       * gets a map from tuple tag to a broadcast helper. The broadcast helper contains a
       * reference to the brpoadcast variable as well as the coder to deserialize it.
       */
      DoFnFunction dofn = new DoFnFunction(transform.getFn(), context);
      context.setOutputRDD(transform, context.getInputRDD(transform).mapPartitions(dofn));
    }
  };

  private static FieldGetter MULTIDO_FG = new FieldGetter(ParDo.BoundMulti.class);
  private static TransformEvaluator<ParDo.BoundMulti> MULTIDO = new TransformEvaluator<ParDo
      .BoundMulti>() {
    @Override
    public void evaluate(ParDo.BoundMulti transform, EvaluationContext context) {
      MultiDoFnFunction multifn = new MultiDoFnFunction(
          transform.getFn(),
          context,
          (TupleTag) MULTIDO_FG.get("mainOutputTag", transform));

      JavaPairRDD<TupleTag, Object> all = context.getInputRDD(transform)
          .mapPartitionsToPair(multifn)
          .cache();

      PCollectionTuple pct = (PCollectionTuple) context.getOutput(transform);
      for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
        TupleTagFilter filter = new TupleTagFilter(e.getKey());
        JavaPairRDD<TupleTag, Object> filtered = all.filter(filter);
        context.setRDD(e.getValue(), filtered.values());
      }
    }
  };


  private static TransformEvaluator<TextIO.Read.Bound> READ_TEXT = new TransformEvaluator<TextIO
      .Read.Bound>() {
    @Override
    public void evaluate(TextIO.Read.Bound transform, EvaluationContext context) {
      String pattern = transform.getFilepattern();
      JavaRDD rdd = context.getSparkContext().textFile(pattern);
      context.setOutputRDD(transform, rdd);
    }
  };

  private static TransformEvaluator<TextIO.Write.Bound> WRITE_TEXT = new
      TransformEvaluator<TextIO.Write.Bound>() {
    @Override
    public void evaluate(TextIO.Write.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getInputRDD(transform);
      String pattern = transform.getFilenamePrefix();
      last.saveAsTextFile(pattern);
    }
  };

  private static TransformEvaluator<AvroIO.Read.Bound> READ_AVRO = new TransformEvaluator<AvroIO
      .Read.Bound>() {
    @Override
    public void evaluate(AvroIO.Read.Bound transform, EvaluationContext context) {
      String pattern = transform.getFilepattern();
      JavaRDD rdd = context.getSparkContext().textFile(pattern);
      context.setOutputRDD(transform, rdd);
    }
  };

  private static TransformEvaluator<AvroIO.Write.Bound> WRITE_AVRO = new
      TransformEvaluator<AvroIO.Write.Bound>() {
    @Override
    public void evaluate(AvroIO.Write.Bound transform, EvaluationContext context) {
      JavaRDDLike last = context.getInputRDD(transform);
      String pattern = transform.getFilenamePrefix();
      last.saveAsTextFile(pattern);
    }
  };

  private static TransformEvaluator<Create> CREATE = new TransformEvaluator<Create>() {
    @Override
    public void evaluate(Create transform, EvaluationContext context) {
      Iterable elems = transform.getElements();
      Coder coder = ((PCollection) context.getOutput(transform)).getCoder();
      JavaRDD rdd = context.getSparkContext().parallelize(
          CoderHelpers.toByteArrays(elems, coder));
      context.setOutputRDD(transform, rdd.map(CoderHelpers.fromByteFunction(coder)));
    }
  };

  private static TransformEvaluator<View.CreatePCollectionView> CREATE_PCOLL_VIEW = new TransformEvaluator<View.CreatePCollectionView>() {


    @Override
    public void evaluate(View.CreatePCollectionView transform, EvaluationContext context) {
      //This PCollection is either a single element or an interable,
      // so serialize it like an iterable
      Coder<?> inputCoder = context.getCoderRegistry().getDefaultCoder(transform.getInput());
      Coder<?> outputCoder = IterableCoder.of(inputCoder);
      PCollectionView view =  context.<PCollectionView>getOutput(transform);
      PInput input = context.getPipeline().getInput(transform);
      context.setSideInput(view, input, outputCoder);
    }
  };

  private static class TupleTagFilter implements Function<Tuple2<TupleTag, Object>, Boolean> {
    private TupleTag tag;

    public TupleTagFilter(TupleTag tag) {
      this.tag = tag;
    }

    @Override
    public Boolean call(Tuple2<TupleTag, Object> input) throws Exception {
      return tag.equals(input._1());
    }
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator> mEvaluators = Maps
      .newHashMap();

  static {
    mEvaluators.put(TextIO.Read.Bound.class, READ_TEXT);
    mEvaluators.put(TextIO.Write.Bound.class, WRITE_TEXT);
    mEvaluators.put(AvroIO.Read.Bound.class, READ_AVRO);
    mEvaluators.put(AvroIO.Write.Bound.class, WRITE_AVRO);
    mEvaluators.put(ParDo.Bound.class, PARDO);
    mEvaluators.put(ParDo.BoundMulti.class, MULTIDO);
    mEvaluators.put(GroupByKey.GroupByKeyOnly.class, GBK);
    mEvaluators.put(Combine.GroupedValues.class, GROUPED);
    mEvaluators.put(Create.class, CREATE);
    mEvaluators.put(Flatten.FlattenPCollectionList.class, FLATTEN_PCOLLECTION_LIST);
    mEvaluators.put(View.CreatePCollectionView.class, CREATE_PCOLL_VIEW);
  }

  public static TransformEvaluator getTransformEvaluator(Class<? extends PTransform> clazz) {
    TransformEvaluator transform = mEvaluators.get(clazz);
    if (transform == null) {
      throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
    }
    return transform;
  }
}
