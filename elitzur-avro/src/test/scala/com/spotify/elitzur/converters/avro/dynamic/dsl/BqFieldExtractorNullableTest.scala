/*
 * Copyright 2021 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import com.google.common.base.Charsets
import com.spotify.elitzur.converters.avro.dynamic.dsl.bq.BqSchema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.{util => ju}

class BqFieldExtractorNullableTest extends AnyFlatSpec with Matchers {

  private val tableSchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/BqNullableSchema.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  it should "extract a null from an Union schema type" in {
    // Input: {"optRecord": null}
    // Output: null
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".optRecord.optString")
    val testNullRecord = new TableRow().set("optRecord", null)

    fn(testNullRecord) should be (testNullRecord.get("optRecord"))
  }

  it should "extract a null from a nested Union Avro schema type" in {
    // Input: {"optRecord": {"optString": null}}
    // Output: null
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".optRecord.optString")
    val testInnerNullRecord = new TableRow().set("optRecord", new TableRow().set("optString", null))

    fn(testInnerNullRecord) should be (testInnerNullRecord
      .get("optRecord").asInstanceOf[ju.Map[String, Any]].get("optString"))
  }

  it should "extract a primitive from a Union Avro schema type" in {
    // Input: {"optRecord": {"optString": "abc"}}
    // Output: "abc"
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".optRecord.optString")
    val testInnerNonNullRecord = new TableRow().set(
      "optRecord", new TableRow().set("optString", "abc"))

    fn(testInnerNonNullRecord) should be (testInnerNonNullRecord
      .get("optRecord").asInstanceOf[ju.Map[String, Any]].get("optString"))
  }

  it should "return null if child schema is non-nullable" in {
    // Input: {"optRecord": null}
    // Output: "null"
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".optRecord.nonOptString")
    val testNullRecord = new TableRow().set("optRecord", null)

    fn(testNullRecord) should be (testNullRecord.get("optRecord"))
  }

  it should "return the elements of an array if array is not null" in {
    // Input: {"optRecord": {"optRepeatedArray": [{"userId": "a", "countryCode": "US"}]}}
    // Output: "a"

//    val fnArrayNull = AvroObjMapper.getAvroFun(".optRecord.optRepeatedArray[].userId",
//      TestAvroUnionTypes.SCHEMA$)
//    val testInnerNonNullRecord = TestAvroUnionTypes.newBuilder()
//      .setOptRecord(
//        InnerComplexType.newBuilder()
//          .setOptString(null)
//          .setOptRepeatedArray(List(innerNestedSample()).asJava).build()
//      ).build
//
//    fnArrayNull(testInnerNonNullRecord) should be (
//      testInnerNonNullRecord.getOptRecord.getOptRepeatedArray.asScala.map(_.getUserId).asJava)
  }

  it should "return null if array is null" in {
    // Input: {"optRecord": {"optRepeatedArray": null}}
    // Output: null

//    val fnArrayNull = AvroObjMapper.getAvroFun(".optRecord.optRepeatedArray[].userId",
//      TestAvroUnionTypes.SCHEMA$)
//    val testInnerNullRecord = TestAvroUnionTypes.newBuilder()
//      .setOptRecord(
//        InnerComplexType.newBuilder()
//          .setOptString(null)
//          .setOptRepeatedArray(null).build()).build
//
//    fnArrayNull(testInnerNullRecord) should be (
//      testInnerNullRecord.getOptRecord.getOptRepeatedArray)
  }

}
