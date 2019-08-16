/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.model.output.serde;

import com.tencent.angel.model.output.element.IntDoubleElement;
import com.tencent.angel.model.output.element.IntFloatElement;
import com.tencent.angel.model.output.element.IntIntElement;
import com.tencent.angel.model.output.element.IntLongElement;
import com.tencent.angel.model.output.element.LongDoubleElement;
import com.tencent.angel.model.output.element.LongFloatElement;
import com.tencent.angel.model.output.element.LongIntElement;
import com.tencent.angel.model.output.element.LongLongElement;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface ElementSerDe {

  /**
   * serialize a (int, float) element
   *
   * @param element a (int, float) element
   * @param out output stream
   */
  void serialize(IntFloatElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (int, double) element
   *
   * @param element a (int, double) element
   * @param out output stream
   */
  void serialize(IntDoubleElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (int, int) element
   *
   * @param element a (int, int) element
   * @param out output stream
   */
  void serialize(IntIntElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (int, long) element
   *
   * @param element a (int, long) element
   * @param out output stream
   */
  void serialize(IntLongElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (long, float) element
   *
   * @param element a (long, float) element
   * @param out output stream
   */
  void serialize(LongFloatElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (long, double) element
   *
   * @param element a (long, double) element
   * @param out output stream
   */
  void serialize(LongDoubleElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (long, int) element
   *
   * @param element a (long, int) element
   * @param out output stream
   */
  void serialize(LongIntElement element, DataOutputStream out) throws IOException;

  /**
   * serialize a (long, long) element
   *
   * @param element a (long, long) element
   * @param out output stream
   */
  void serialize(LongLongElement element, DataOutputStream out) throws IOException;

  /**
   * deserialize a (int, float) element
   *
   * @param element a (int, float) element
   * @param in input stream
   */
  void deserialize(IntFloatElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (int, double) element
   *
   * @param element a (int, double) element
   * @param in input stream
   */
  void deserialize(IntDoubleElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (int, int) element
   *
   * @param element a (int, int) element
   * @param in input stream
   */
  void deserialize(IntIntElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (int, long) element
   *
   * @param element a (int, long) element
   * @param in input stream
   */
  void deserialize(IntLongElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (long, float) element
   *
   * @param element a (long, float) element
   * @param in input stream
   */
  void deserialize(LongFloatElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (long, double) element
   *
   * @param element a (long, double) element
   * @param in input stream
   */
  void deserialize(LongDoubleElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (long, int) element
   *
   * @param element a (long, int) element
   * @param in input stream
   */
  void deserialize(LongIntElement element, DataInputStream in) throws IOException;

  /**
   * deserialize a (long, long) element
   *
   * @param element a (long, long) element
   * @param in input stream
   */
  void deserialize(LongLongElement element, DataInputStream in) throws IOException;
}
