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

@Iri("http://purl.oclc.org/NET/ssnx/ssn#Sensor")
trait Sensor {

  @Iri("http://www.w3.org/2000/01/rdf-schema#comment")
  def comment(): String;
  def comment(comment: String): Sensor;

}