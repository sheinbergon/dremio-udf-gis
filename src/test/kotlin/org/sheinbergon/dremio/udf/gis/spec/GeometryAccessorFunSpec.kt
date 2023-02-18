package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import com.dremio.exec.expr.fn.impl.StringFunctionHelpers
import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestScope
import io.kotest.matchers.doubles.shouldBeExactly
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.Float8Holder
import org.apache.arrow.vector.holders.IntHolder
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableIntHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
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
    value: Number
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
    value: String
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
    value: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
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
    value: Boolean,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
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

  protected fun testNullGeometryAccessor(
    name: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
    function.apply {
      wkbInput.isSet = 0
      setup()
      eval()
      output.isNotSet()
    }
  }

  protected fun testThrowingGeometryAccessor(
    name: String,
    wkt: String,
    message: String
  ) = test(name) {
    shouldThrowMessage(message) {
      function.apply {
        wkbInput.setFromWkt(wkt)
        setup()
        eval()
      }
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
    is NullableVarCharHolder -> this.valueIsSetTo(value as String)
    is NullableFloat8Holder -> this.valueIsSetTo(value as Double)
    is NullableIntHolder -> this.valueIsSetTo(value as Int)
    is NullableBitHolder -> this.valueIsSetTo(value as Boolean)
    is Float8Holder -> this.valueIsSetTo(value as Double)
    is IntHolder -> this.valueIsSetTo(value as Int)
    is BitHolder -> this.valueIsSetTo(value as Boolean)
    else -> throw IllegalArgumentException("Unsupported value holder type '${this::class.simpleName}'")
  }

  private fun O.isNotSet() = when(this) {
    is NullableVarCharHolder -> this.valueIsNotSet()
    is NullableIntHolder -> this.valueIsNotSet()
    is NullableFloat8Holder -> this.valueIsNotSet()
    is NullableBitHolder -> this.valueIsNotSet()
    else -> throw IllegalArgumentException("Unsupported value holder type '${this::class.simpleName}'")
  }

  private fun O.reset() = when(this) {
    is NullableVarCharHolder -> (this as NullableVarCharHolder).reset()
    is NullableFloat8Holder -> (this as NullableFloat8Holder).reset()
    is NullableBitHolder -> (this as NullableBitHolder).reset()
    is NullableIntHolder -> (this as NullableIntHolder).reset()
    is Float8Holder -> (this as Float8Holder).reset()
    is BitHolder -> (this as BitHolder).reset()
    is IntHolder -> (this as IntHolder).reset()
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

  private fun NullableVarCharHolder.valueIsSetTo(text: String) {
    run {
      isSet shouldBeExactly 1
      StringFunctionHelpers.getStringFromNullableVarCharHolder(this) shouldBe text
    }
  }

  private fun Float8Holder.valueIsSetTo(double: Double) {
    run {
      value shouldBeExactly double
    }
  }

  private fun IntHolder.valueIsSetTo(int: Int) {
    run {
      value shouldBeExactly int
    }
  }

  private fun NullableIntHolder.valueIsSetTo(int: Int) {
    run {
      isSet shouldBeExactly 1
      value shouldBeExactly int
    }
  }

  private fun NullableFloat8Holder.valueIsNotSet() {
    run {
      isSet shouldBeExactly 0
      value shouldBeExactly 0.0
    }
  }

  private fun NullableVarCharHolder.valueIsNotSet() {
    run {
      isSet shouldBeExactly 0
      buffer shouldBe null
    }
  }

  private fun NullableBitHolder.valueIsNotSet() {
    run {
      isSet shouldBeExactly 0
      value shouldBeExactly 0
    }
  }

  private fun NullableIntHolder.valueIsNotSet() {
    run {
      isSet shouldBeExactly 0
      value shouldBeExactly 0
    }
  }

  private fun BitHolder.valueIsSetTo(value: Boolean) {
    run {
      this.value shouldBeExactly if (value) 1 else 0
    }
  }

  private fun NullableBitHolder.valueIsSetTo(value: Boolean) {
    run {
      isSet shouldBeExactly 1
      this.value shouldBeExactly if (value) 1 else 0
    }
  }
}
