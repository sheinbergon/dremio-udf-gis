package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestScope
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.arrow.vector.holders.ValueHolder
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsFalse
import org.sheinbergon.dremio.udf.gis.util.valueIsTrue

abstract class GeometryRelationFunSpec<F : SimpleFunction, O : ValueHolder> : FunSpec() {

  abstract class NullableVarCharOutput<F : SimpleFunction> : GeometryRelationFunSpec<F, NullableVarCharHolder>() {

    init {
      beforeEach {
        function.output.reset()
      }
    }

    protected fun testGeometryRelation(
      name: String,
      wkt1: String,
      wkt2: String,
      result: String
    ) = test(name) {
      function.apply {
        wkbInput1.setFromWkt(wkt1)
        wkbInput2.setFromWkt(wkt2)
        setup()
        eval()
        output.valueIs(result)
      }
    }

    private fun NullableVarCharHolder.valueIs(text: String) =
      GeometryHelpers.toUTF8String(this) shouldBe text

    override fun NullableVarCharHolder.valueIsNotSet() =
      this.isSet shouldBe 0
  }

  abstract class NullableBitOutput<F : SimpleFunction> : GeometryRelationFunSpec<F, NullableBitHolder>() {

    init {
      beforeEach {
        function.output.reset()
      }
    }

    protected fun testTrueGeometryRelation(
      name: String,
      wkt1: String,
      wkt2: String,
      precursor: suspend TestScope.() -> Unit = {}
    ) = test(name) {
      precursor(this)
      function.apply {
        wkbInput1.setFromWkt(wkt1)
        wkbInput2.setFromWkt(wkt2)
        setup()
        eval()
        output.valueIsTrue()
      }
    }

    protected fun testFalseGeometryRelation(
      name: String,
      wkt1: String,
      wkt2: String,
      precursor: suspend TestScope.() -> Unit = {}
    ) = test(name) {
      precursor(this)
      function.apply {
        wkbInput1.setFromWkt(wkt1)
        wkbInput2.setFromWkt(wkt2)
        setup()
        eval()
        output.valueIsFalse()
      }
    }

    override fun NullableBitHolder.valueIsNotSet() =
      this.isSet shouldBe 0
  }

  protected fun testNullGeometryRelation(
    name: String,
    wkt1: String? = null,
    wkt2: String? = null,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
    function.apply {
      wkt1?.also { wkbInput1.setFromWkt(wkt1) }
      wkt2?.also { wkbInput2.setFromWkt(wkt2) }
      setup()
      eval()
      output.valueIsNotSet()
    }
  }

  protected fun testDifferentSRIDGeometryRelation(
    name: String,
    wkt1: String,
    wkt2: String,
    srid1: Int? = null,
    srid2: Int? = null,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    shouldThrow<IllegalArgumentException> {
      precursor(this)
      function.apply {
        wkbInput1.setFromWkt(wkt1, srid1)
        wkbInput2.setFromWkt(wkt2, srid2)
        setup()
        eval()
      }
    }
  }

  init {
    beforeEach {
      function.wkbInput1.reset()
      function.wkbInput2.reset()
    }

    afterEach {
      function.wkbInput1.release()
      function.wkbInput2.release()
    }
  }

  protected abstract fun O.valueIsNotSet()
  protected abstract val function: F
  protected abstract val F.wkbInput1: NullableVarBinaryHolder
  protected abstract val F.wkbInput2: NullableVarBinaryHolder
  protected abstract val F.output: O
}
