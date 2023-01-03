package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestScope
import io.kotest.matchers.doubles.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.NullableFloat8Holder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt

abstract class GeometryMeasurementFunSpec<F : SimpleFunction> : FunSpec() {

  abstract class Quaternary<F : SimpleFunction> : Ternary<F>() {

    init {
      beforeEach {
        function.wkbInput4.reset()
      }

      afterEach {
        function.wkbInput4.release()
      }
    }

    protected fun testGeometryMeasurement(
      name: String,
      wkt1: String,
      wkt2: String,
      wkt3: String,
      wkt4: String,
      value: Double
    ) = test(name) {
      function.apply {
        wkbInput1.setFromWkt(wkt1)
        wkbInput2.setFromWkt(wkt2)
        wkbInput3.setFromWkt(wkt3)
        wkbInput4.setFromWkt(wkt4)
        setup()
        eval()
        measurementOutput.isSetTo(value)
      }
    }

    protected fun testNullGeometryMeasurement(
      name: String,
      wkt1: String? = null,
      wkt2: String? = null,
      wkt3: String? = null,
      wkt4: String? = null,
      srid: Int,
    ) = test(name) {
      function.apply {
        wkt1?.also { wkbInput1.setFromWkt(wkt1, srid) }
        wkt2?.also { wkbInput2.setFromWkt(wkt2, srid) }
        wkt3?.also { wkbInput2.setFromWkt(wkt3, srid) }
        wkt4?.also { wkbInput2.setFromWkt(wkt4, srid) }
        setup()
        eval()
        measurementOutput.valueIsNotSet()
      }
    }

    protected abstract val F.wkbInput4: NullableVarBinaryHolder
  }

  abstract class Ternary<F : SimpleFunction> : Binary<F>() {

    init {
      beforeEach {
        function.wkbInput3.reset()
      }

      afterEach {
        function.wkbInput3.release()
      }
    }

    protected fun testGeometryMeasurement(
      name: String,
      wkt1: String,
      wkt2: String,
      wkt3: String,
      value: Double
    ) = test(name) {
      function.apply {
        wkbInput1.setFromWkt(wkt1)
        wkbInput2.setFromWkt(wkt2)
        wkbInput3.setFromWkt(wkt3)
        setup()
        eval()
        measurementOutput.isSetTo(value)
      }
    }

    protected fun testNullGeometryMeasurement(
      name: String,
      wkt1: String? = null,
      wkt2: String? = null,
      wkt3: String? = null,
      srid: Int,
    ) = test(name) {
      function.apply {
        wkt1?.also { wkbInput1.setFromWkt(wkt1, srid) }
        wkt2?.also { wkbInput2.setFromWkt(wkt2, srid) }
        wkt3?.also { wkbInput2.setFromWkt(wkt3, srid) }
        setup()
        eval()
        measurementOutput.valueIsNotSet()
      }
    }

    protected abstract val F.wkbInput3: NullableVarBinaryHolder
  }

  abstract class Binary<F : SimpleFunction> : Unary<F>() {

    init {
      beforeEach {
        function.wkbInput2.reset()
      }

      afterEach {
        function.wkbInput2.release()
      }
    }

    protected fun testGeometryMeasurement(
      name: String,
      wkt1: String,
      wkt2: String,
      value: Double
    ) = test(name) {
      function.apply {
        wkbInput1.setFromWkt(wkt1)
        wkbInput2.setFromWkt(wkt2)
        setup()
        eval()
        measurementOutput.isSetTo(value)
      }
    }

    protected fun testGeometryMeasurement(
      name: String,
      wkt1: String,
      wkt2: String,
      srid: Int,
      value: Double
    ) = test(name) {
      function.apply {
        wkbInput1.setFromWkt(wkt1, srid)
        wkbInput2.setFromWkt(wkt2, srid)
        setup()
        eval()
        measurementOutput.isSetTo(value)
      }
    }

    protected fun testNullGeometryMeasurement(
      name: String,
      wkt1: String? = null,
      wkt2: String? = null,
      srid: Int,
    ) = test(name) {
      function.apply {
        wkt1?.also { wkbInput1.setFromWkt(wkt1, srid) }
        wkt2?.also { wkbInput2.setFromWkt(wkt2, srid) }
        setup()
        eval()
        measurementOutput.valueIsNotSet()
      }
    }

    protected abstract val F.wkbInput2: NullableVarBinaryHolder
  }

  abstract class Unary<F : SimpleFunction> : GeometryMeasurementFunSpec<F>() {

    init {
      beforeEach {
        function.measurementOutput.reset()
        function.wkbInput1.reset()
      }

      afterEach {
        function.wkbInput1.release()
      }
    }

    protected fun testGeometryMeasurement(
      name: String,
      wkt1: String,
      srid: Int,
      value: Double
    ) = test(name) {
      function.apply {
        wkbInput1.setFromWkt(wkt1, srid)
        setup()
        eval()
        measurementOutput.isSetTo(value)
      }
    }

    protected fun testNullGeometryMeasurement(
      name: String,
      precursor: suspend TestScope.() -> Unit = {}
    ) = test(name) {
      precursor(this)
      function.apply {
        GeometryHelpers.markHolderNotSet(wkbInput1)
        setup()
        eval()
        measurementOutput.valueIsNotSet()
      }
    }

    protected abstract val F.wkbInput1: NullableVarBinaryHolder
  }

  protected abstract val function: F
  protected abstract val F.measurementOutput: NullableFloat8Holder

  protected fun NullableFloat8Holder.isSetTo(double: Double) {
    run {
      value shouldBeExactly double
      isSet shouldBe 1
    }
  }

  protected fun NullableFloat8Holder.valueIsNotSet() {
    run {
      isSet shouldBe 0
    }
  }
}
