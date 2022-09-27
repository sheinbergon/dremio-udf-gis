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
import org.sheinbergon.dremio.udf.gis.util.setUtf8

abstract class GeometryInputFunSpec<F : SimpleFunction, I : ValueHolder> : FunSpec() {

  abstract class NullableVarChar<F : SimpleFunction> : GeometryInputFunSpec<F, NullableVarCharHolder>() {
    init {
      beforeEach {
        function.input.reset()
      }

      afterEach {
        function.input.release()
      }
    }

    protected fun testGeometryInput(
      name: String,
      text: String,
      result: ByteArray
    ) = test(name) {
      function.apply {
        input.setUtf8(text)
        setup()
        eval()
        output.valueIs(result)
      }
    }

    protected fun testGeometryInput(
      name: String,
      text: String,
      result: ByteArray,
      precursor: suspend TestScope.() -> Unit = {}
    ) = test(name) {
      precursor.invoke(this)
      function.apply {
        input.setUtf8(text)
        setup()
        eval()
        output.valueIs(result)
      }
    }


    protected fun testInvalidGeometryInput(
      name: String,
      text: String,
    ) = test(name) {
      shouldThrowAny {
        function.apply {
          input.setUtf8(text)
          setup()
          eval()
        }
      }
    }
  }

  init {
    beforeEach {
      function.output.reset()
    }
  }

  protected fun NullableVarBinaryHolder.valueIs(bytes: ByteArray) =
    GeometryHelpers.toBinary(GeometryHelpers.toGeometry(this)) shouldBe bytes

  protected abstract val function: F
  protected abstract val F.input: I
  protected abstract val F.output: NullableVarBinaryHolder
}
