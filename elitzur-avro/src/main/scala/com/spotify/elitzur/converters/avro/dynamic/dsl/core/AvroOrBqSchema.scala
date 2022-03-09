package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import org.apache.avro.Schema
import com.spotify.elitzur.converters.avro.dynamic.dsl.bq.BqSchema
import org.apache.avro.generic.GenericRecord

import java.{util => ju}

sealed trait AvroOrBqSchema[T]

object AvroOrBqSchema {
  implicit val avroSchema: AvroOrBqSchema[Schema] =
    new AvroOrBqSchema[Schema] {}
  implicit val bqSchema: AvroOrBqSchema[BqSchema] =
    new AvroOrBqSchema[BqSchema] {}
}

object AvroOrBqSchemaUtil {
  def getAvroOrBqFieldObject[T: AvroOrBqSchema](
    t: T, fieldName: String): Any => Any = t match {
    case _: Schema => (o: Any) => o.asInstanceOf[GenericRecord].get(fieldName)
    case _: BqSchema => (o: Any) => o.asInstanceOf[ju.Map[String, Any]].get(fieldName)
  }
}
