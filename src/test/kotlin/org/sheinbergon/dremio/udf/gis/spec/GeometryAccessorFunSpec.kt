package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.doubles.shouldBeExactly
import io.kotest.matchers.ints.shouldBeExactly
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.ValueHolder
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt

abstract class GeometryAccessorFunSpec<F : SimpleFunction, O : ValueHolder> : FunSpec() {

  init {
    beforeEach {
      function.output.reset()
      function.wkbInput.reset()
    }

    afterEach {
      function.wkbInput.release()
    }
  }

  protected fun testGeometryAccessor(
    name: String,
    wkt: String,
    value: Double
  ) = test(name) {
    function.apply {
      wkbInput.setFromWkt(wkt)
      setup()
      eval()
      output.isSetTo(value)
    }
  }

  protected fun testGeometryAccessor(
    name: String,
    wkt: String,
    value: Boolean
  ) = test(name) {
    function.apply {
      wkbInput.setFromWkt(wkt)
      setup()
      eval()
      output.isSetTo(value)
    }
  }

  protected fun testNoResultGeometryAccessor(
    name: String,
    wkt: String
  ) = test(name) {
    function.apply {
      wkbInput.setFromWkt(wkt)
      setup()
      eval()
      output.isNotSet()
    }
  }

  private fun O.isSetTo(value: Any) = when(this) {
    is NullableFloat8Holder -> this.valueIsSetTo(value as Double)
    is Float8Holder -> this.valueIsSetTo(value as Double)
    is BitHolder -> this.valueIsSetTo(value as Boolean)
    else -> throw IllegalArgumentException("Unsupported value holder type '${this::class.simpleName}'")
  }

  private fun O.isNotSet() = when(this) {
    is NullableFloat8Holder -> this.valueIsNotSet()
    else -> throw IllegalArgumentException("Unsupported value holder type '${this::class.simpleName}'")
  }

  private fun O.reset() = when(this) {
    is NullableFloat8Holder -> (this as NullableFloat8Holder).reset()
    is Float8Holder -> (this as Float8Holder).reset()
    is BitHolder -> (this as BitHolder).reset()
    else -> throw IllegalArgumentException("Unsupported value holder type '${this::class.simpleName}'")
  }

  protected abstract val function: F
  protected abstract val F.wkbInput: NullableVarBinaryHolder
  protected abstract val F.output: O

  private fun NullableFloat8Holder.valueIsSetTo(double: Double) {
    run {
      isSet shouldBeExactly 1
      value shouldBeExactly double
    }
  }

  private fun Float8Holder.valueIsSetTo(double: Double) {
    run {
      value shouldBeExactly double
    }
  }

  private fun NullableFloat8Holder.valueIsNotSet() {
    run {
      isSet shouldBeExactly 0
      value shouldBeExactly 0.0
    }
  }

  private fun BitHolder.valueIsSetTo(value: Boolean) {
    run {
      if (value) this.value shouldBeExactly 1
      else this.value shouldBeExactly 0
    }
  }
}
