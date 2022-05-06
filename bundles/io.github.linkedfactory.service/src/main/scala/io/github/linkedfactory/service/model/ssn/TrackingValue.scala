/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.service.model.ssn

import net.enilink.composition.annotations.Iri

@Iri("http://www.w3.org/2003/01/geo/wgs84_pos#Point")
trait TrackingValue {

  @Iri("http://www.w3.org/2003/01/geo/wgs84_pos#lat")
  def lat(): Double
  def lat(value: Double): TrackingValue

  @Iri("http://www.w3.org/2003/01/geo/wgs84_pos#long")
  def lng(): Double
  def lng(value: Double): TrackingValue

  @Iri("http://www.w3.org/2003/01/geo/wgs84_pos#x")
  def x(): Double
  def x(value: Double): TrackingValue

  @Iri("http://www.w3.org/2003/01/geo/wgs84_pos#y")
  def y(): Double
  def y(value: Double): TrackingValue
}