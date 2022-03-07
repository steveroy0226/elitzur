package com.spotify.elitzur.converters.avro.dynamic.dsl.avro

object AvroAccessorException {
  class InvalidDynamicFieldException(msg: String) extends Exception(msg)

  final val MISSING_TOKEN =
    "Leading '.' missing in the arg. Please prepend '.' to the arg"

  // TODO: Update docs on Dynamic and Magnolia based Elitzur and link it to exception below
  final val UNSUPPORTED_MAP_SCHEMA =
    "Map schema not supported. Please use Magnolia version of Elitzur."

  final val INVALID_UNION_SCHEMA =
    "Union schemas containing more than one non-null schemas is not supported."

  final val MISSING_ARRAY_TOKEN =
    """
      |Missing `[]` token for an array fields. All array fields should have `[]` token provided
      |in the input.
      |""".stripMargin

}
