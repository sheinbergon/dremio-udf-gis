package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.AggrFunction
import io.kotest.core.spec.style.FunSpec
import org.apache.arrow.vector.holders.NullableBitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedIn
import org.sheinbergon.dremio.udf.gis.util.valueIsFalse
import org.sheinbergon.dremio.udf.gis.util.valueIsTrue

abstract class GeometryAggregationFunSpec<F : AggrFunction> : FunSpec() {

  protected fun testGeometryAggregration(
    name: String,
    wkts: Array<String>,
    expected: String
  ) = test(name) {
    function.apply {
      setup()
      reset()
      wkts.forEach { wkt ->
        function.wkbInput.reset()
        wkbInput.setFromWkt(wkt)
        add()
      }
      output()
      wkbOutput.valueIsAsDescribedIn(expected)
      setIndicator.valueIsTrue()
    }
  }

  protected fun testGeometryAggregationNoInput(
    name: String,
  ) = test(name) {
    function.apply {
      setup()
      reset()
      output()
      setIndicator.valueIsFalse()
    }
  }

  init {
    beforeEach {
      function.wkbOutput.reset()
      function.aggregationValue.reset()
      function.setIndicator.reset()
    }

    afterEach {
      function.wkbInput.release()
    }
  }

  protected abstract val function: F

  protected abstract val F.wkbInput: NullableVarBinaryHolder
  protected abstract val F.wkbOutput: NullableVarBinaryHolder
  protected abstract val F.aggregationValue: NullableVarBinaryHolder
  protected abstract val F.setIndicator: NullableBitHolder
}
