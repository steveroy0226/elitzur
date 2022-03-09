package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.dsl.DynamicImplicits.AccessorFunctionUtils
import com.spotify.elitzur.converters.avro.dynamic.dsl.avro.AvroAccessorUtil
import com.spotify.elitzur.converters.avro.dynamic.dsl.bq.{BqAccessorUtil, BqSchema}
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.AvroOrBqSchema
import org.apache.avro.Schema

import scala.collection.mutable

class FieldAccessor[T: AvroOrBqSchema](schema: T) {
  private val mapToAccessor: mutable.Map[String, Any => Any] = mutable.Map.empty[String, Any => Any]

  def getFieldAccessor(fieldPath: String): Any => Any = {
    if (!mapToAccessor.contains(fieldPath)) {
      val accessorOps = schema match {
        case avro: Schema =>
          AvroAccessorUtil.getFieldAccessorOps(fieldPath, avro).map(_.ops)
        case bq: BqSchema =>
          BqAccessorUtil.getFieldAccessorOps(fieldPath, bq).map(_.ops)
      }
      mapToAccessor += (fieldPath -> accessorOps.combineFns)
    }
    mapToAccessor(fieldPath)
  }

}
