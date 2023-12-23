package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestScope
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.arrow.vector.holders.ValueHolder
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setBinary
import org.sheinbergon.dremio.udf.gis.util.setUtf8
import org.sheinbergon.dremio.udf.gis.util.valueIsNotSet
import java.util.*

abstract class GeometryInputFunSpec<F : SimpleFunction, I : ValueHolder, V : Any> : FunSpec() {

  abstract class NullableVarBinary<F : SimpleFunction> : GeometryInputFunSpec<F, NullableVarBinaryHolder, ByteArray>() {
    init {
      beforeEach {
        function.input.reset()
      }

      afterEach {
        function.input.release()
      }
    }

    final override fun NullableVarBinaryHolder.markNotSet() = this.valueIsNotSet()

    final override fun NullableVarBinaryHolder.set(value: ByteArray) = this.setBinary(value)
  }

  abstract class NullableVarChar<F : SimpleFunction> : GeometryInputFunSpec<F, NullableVarCharHolder, String>() {
    init {
      beforeEach {
        function.input.reset()
      }

      afterEach {
        function.input.release()
      }
    }

    final override fun NullableVarCharHolder.markNotSet() {
      this.valueIsNotSet()
    }

    final override fun NullableVarCharHolder.set(value: String) = this.setUtf8(value)
  }

  init {
    beforeEach {
      function.output.reset()
    }
  }

  protected fun testGeometryInput(
    name: String,
    value: V,
    result: ByteArray,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor.invoke(this)
    function.apply {
      input.set(value)
      setup()
      eval()
      output.valueIs(result)
    }
  }

  protected fun testNullGeometryInput(
    name: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor.invoke(this)
    function.apply {
      input.markNotSet()
      setup()
      eval()
      output.valueIsNotSet()
    }
  }

  protected fun testInvalidGeometryInput(
    name: String,
    value: V,
  ) = test(name) {
    shouldThrowAny {
      function.apply {
        input.set(value)
        setup()
        eval()
      }
    }
  }

  private fun NullableVarBinaryHolder.valueIs(bytes: ByteArray) =
    print(Arrays.toString(GeometryHelpers.toEWKB(GeometryHelpers.toGeometry(this))))
      .also {
        GeometryHelpers.toEWKB(GeometryHelpers.toGeometry(this)) shouldBe bytes
      }

  protected abstract fun I.set(value: V)
  protected abstract val function: F
  protected abstract val F.input: I
  protected abstract fun I.markNotSet()
  protected abstract val F.output: NullableVarBinaryHolder
}
