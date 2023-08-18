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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;

import org.apache.commons.lang3.StringUtils;

import static java.util.Objects.requireNonNull;

public interface EnumerableTestBase {

  default CalciteAssert.AssertThat buildAssertThat(
      CalciteConnectionProperty property,
      Lex lex,
      Boolean forceDecorrelate,
      String name,
      Object schema) {
    CalciteAssert.AssertThat initAssert = CalciteAssert.that();

    if (property != null) {
      requireNonNull(lex, "Lex cannot be null if property is not null");
      initAssert = initAssert.with(property, lex);
    } else if (forceDecorrelate != null) {
      initAssert = initAssert.with(CalciteConnectionProperty.FORCE_DECORRELATE, forceDecorrelate);
    } else if (StringUtils.isNotEmpty(name)) {
      requireNonNull(schema, "Schema cannot be null if name is not empty");
      initAssert = initAssert.withSchema(name, new ReflectiveSchema(schema));
    }
    return initAssert;
  }

  default CalciteAssert.AssertThat defaultAssertThat() {
    return buildAssertThat(CalciteConnectionProperty.LEX, Lex.JAVA, false, null, null);
  }
}
