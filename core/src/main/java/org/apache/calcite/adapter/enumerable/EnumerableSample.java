/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Sample} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableSample
    extends SingleRel
    implements EnumerableRel {
  private final RelOptSamplingParameters params;

  public EnumerableSample(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      RelOptSamplingParameters params) {
    super(cluster, traits, input);
    this.params = params;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableSample(getCluster(), traitSet, sole(inputs), params);
  }

  /**
   * Creates an EnumerableSample.
   */
  public static EnumerableSample create(RelNode input, RelOptSamplingParameters params) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = input.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return new EnumerableSample(cluster, traitSet, input, params);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);

    final PhysType physType =
        PhysTypeImpl.of(
            typeFactory, getRowType(), pref.prefer(result.format));

    Type inputJavaType = result.physType.getJavaRowType();

    ParameterExpression inputEnumerator =
        Expressions.parameter(
            Types.of(
                Enumerator.class, inputJavaType),
            "inputEnumerator");

    Expression input =
        EnumUtils.convert(
            Expressions.call(
                inputEnumerator,
                BuiltInMethod.ENUMERATOR_CURRENT.method),
            inputJavaType);

    Pair<Integer, Integer> value = xxxx(params.sampleRate);
    boolean isOne = value.left == 1 ? true : false;

    Expression v1 = Expressions.constant(isOne ? 0 : value.left);
    Expression v2 = Expressions.constant(value.right);

    if (params.isBernoulli()) {
      Expression operator = params.isRepeatable()
          ? Expressions.call(BuiltInMethod.RAND_INTEGER_SEED.method,
          Expressions.constant(params.getRepeatableSeed()), v2)
          : Expressions.call(BuiltInMethod.RAND_INTEGER.method, v2);

      Expression condition = isOne ? Expressions.equal(operator,v1) :
      Expressions.lessThan(operator,
          v1);

      final BlockBuilder builder2 = new BlockBuilder();

      builder2.add(
          Expressions.ifThen(
              condition,
              Expressions.return_(
                  null, Expressions.constant(true))));

      return implementor.result(
          physType, Expressions.block(
          Expressions.while_(
              Expressions.call(
                  inputEnumerator,
                  BuiltInMethod.ENUMERATOR_MOVE_NEXT.method),
              builder2.toBlock()),
          Expressions.return_(
              null,
              Expressions.constant(false))));
    } else {
      // Now we don't support the system table sample.
      throw new UnsupportedOperationException();
    }
  }

  public Pair<Integer, Integer> xxxx(BigDecimal decimal) {
    String str = decimal.stripTrailingZeros().toPlainString();
    int scale = str.length() - 1 - str.indexOf(".");
    int numerator = Integer.parseInt(str.replace(".", ""));
    int denominator = (int) Math.pow(10, scale);

    // 简化分数
    BigInteger gcd = BigInteger.valueOf(numerator).gcd(BigInteger.valueOf(denominator));
    numerator /= gcd.intValue();
    denominator /= gcd.intValue();

    return Pair.of(numerator, denominator);
  }
}
