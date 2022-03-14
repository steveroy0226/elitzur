package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.dsl.core._

import java.{util => ju}

object DynamicImplicits {
  implicit class AccessorFunctionUtils(val fns: List[BaseAccessor]) {
    private[dsl] def combineFns: Any => Any =
      if (!containsArrayAccessor) {
        fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
      } else {
        fnWithArray
      }

    private[dsl] def fnWithArray: Any => Any = (o: Any) => {
      val res = new ju.ArrayList[Any]
      val f = fns.supplyGlobalArrayToAccessors(res)
        .map(_.fn)
        .reduceLeftOption((f, g) => f andThen g)
        .getOrElse(NoopAccessor().fn)
      f(o)
      res
    }

    private[dsl] def containsArrayAccessor: Boolean = {
      fns.foldLeft(false)((accBoolean, currAccessor) => {
        val hasArrayAccessor = currAccessor match {
          case n: NullableIndexAccessor => n.innerOps.containsArrayAccessor
          case n: NullableGenericAccessor => n.innerOps.containsArrayAccessor
          case _: ArrayMapAccessor | _: ArrayFlatmapAccessor => true
          case a: ArrayNoopAccessor => a.flatten
          case _ => false
        }
        accBoolean || hasArrayAccessor
      })
    }

    private[dsl] def supplyGlobalArrayToAccessors(array: ju.List[Any]): List[BaseAccessor] = {
      fns.map {
        case n: NullableIndexAccessor =>
          val updatedInnerOps = n.innerOps.supplyGlobalArrayToAccessors(array)
          n.copy(innerFn = updatedInnerOps.combineFns)
        case n: NullableGenericAccessor =>
          val updatedInnerOps = n.innerOps.supplyGlobalArrayToAccessors(array)
          n.copy(innerFn = updatedInnerOps.combineFns)
        case a: ArrayFlatmapAccessor =>
          val updatedInnerOps = a.innerOps.supplyGlobalArrayToAccessors(array)
          a.copy(array = array, innerFn = updatedInnerOps.combineFns)
        case a: ArrayMapAccessor =>
          val updatedInnerOps = a.innerOps.supplyGlobalArrayToAccessors(array)
          a.copy(array = array, innerFn = updatedInnerOps.combineFns)
        case a: ArrayNoopAccessor if a.flatten =>
          a.copy(array = array)
        case x: BaseAccessor => x
      }
    }

  }


}
