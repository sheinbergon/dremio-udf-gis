/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sheinbergon.dremio.udf.gis;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.esri.core.geometry.VertexGeometryAccessor;

import javax.annotation.Nonnull;
import javax.inject.Inject;

@FunctionTemplate(
    name = "ST_Transform",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STTransform implements SimpleFunction {

  private static final String CRS_TEMPLATE = "EPSG: %d";

  @Param
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryInput;

  @Param
  org.apache.arrow.vector.holders.NullableIntHolder sourceSridInput;

  @Param
  org.apache.arrow.vector.holders.NullableIntHolder targetSridInput;

  @Workspace
  org.locationtech.proj4j.CoordinateTransform transformation;

  @Workspace
  org.locationtech.proj4j.CRSFactory crsFactory;

  @Workspace
  int targetSrid;

  @Output
  org.apache.arrow.vector.holders.NullableVarBinaryHolder binaryOutput;

  @Inject
  org.apache.arrow.memory.ArrowBuf buffer;

  public void setup() {
    int sourceSrid = sourceSridInput.value;
    targetSrid = targetSridInput.value;
    org.locationtech.proj4j.CRSFactory factory = new org.locationtech.proj4j.CRSFactory();
    org.locationtech.proj4j.CoordinateReferenceSystem sourceCrs = factory.createFromName(String.format(CRS_TEMPLATE, sourceSrid));
    org.locationtech.proj4j.CoordinateReferenceSystem targetCrs = factory.createFromName(String.format(CRS_TEMPLATE, targetSrid));
    transformation = new org.locationtech.proj4j.BasicCoordinateTransform(sourceCrs, targetCrs);
  }

  public void eval() {
    com.esri.core.geometry.ogc.OGCGeometry geom = FunctionHelpersXL.toGeometry(binaryInput);
    org.locationtech.proj4j.ProjCoordinate target = new org.locationtech.proj4j.ProjCoordinate();
    com.esri.core.geometry.SpatialReference targetSridReference = com.esri.core.geometry.SpatialReference.create(targetSrid);
    com.esri.core.geometry.ogc.OGCGeometry transformed = transformGeometry(geom, target, targetSridReference);
    byte[] bytes = FunctionHelpersXL.toBinary(transformed);
    buffer = buffer.reallocIfNeeded(bytes.length);
    FunctionHelpersXL.populate(bytes, buffer, binaryOutput);

  }

  private com.esri.core.geometry.ogc.OGCGeometry transformGeometry(
      final @Nonnull com.esri.core.geometry.ogc.OGCGeometry geometry,
      final @Nonnull org.locationtech.proj4j.ProjCoordinate to,
      final @Nonnull com.esri.core.geometry.SpatialReference srid) {
    if (FunctionHelpersXL.isAPoint(geometry)) {
      return transform((com.esri.core.geometry.ogc.OGCPoint) geometry, to, srid);
    } else {
      return transform(geometry, to, srid);
    }
  }


  private com.esri.core.geometry.ogc.OGCGeometry transform(
      final @Nonnull com.esri.core.geometry.ogc.OGCPoint point,
      final @Nonnull org.locationtech.proj4j.ProjCoordinate target,
      final @Nonnull com.esri.core.geometry.SpatialReference sridReference) {
    org.locationtech.proj4j.ProjCoordinate source = new org.locationtech.proj4j.ProjCoordinate(point.X(), point.Y());
    org.locationtech.proj4j.ProjCoordinate projection = transformation.transform(source, target);
    return new com.esri.core.geometry.ogc.OGCPoint(new com.esri.core.geometry.Point(projection.x, projection.y), sridReference);

  }

  private com.esri.core.geometry.ogc.OGCGeometry transform(
      final @Nonnull com.esri.core.geometry.ogc.OGCGeometry geometry,
      final @Nonnull org.locationtech.proj4j.ProjCoordinate target,
      final @Nonnull com.esri.core.geometry.SpatialReference sridReference) {
    com.esri.core.geometry.Geometry esriGeometry = geometry.getEsriGeometry();
    com.esri.core.geometry.MultiVertexGeometry vertex = VertexGeometryAccessor.getVertexGeometry(esriGeometry);
    for (int i = 0; i < vertex.getPointCount(); i++) {
      com.esri.core.geometry.Point point = vertex.getPoint(i);
      org.locationtech.proj4j.ProjCoordinate source = new org.locationtech.proj4j.ProjCoordinate(point.getX(), point.getY());
      org.locationtech.proj4j.ProjCoordinate projection = transformation.transform(source, target);
      point.setXY(projection.x, projection.y);
      vertex.setPoint(i, point);
    }
    return com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry(esriGeometry, sridReference);
  }
}
